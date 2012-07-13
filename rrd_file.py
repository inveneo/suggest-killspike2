#!/usr/bin/env python

# rrd_file.py - data structure for manipulating RRD files
# written by jwiggins@inveneo.org June 2012

import os
import sys
import math
import tempfile
from subprocess import Popen, PIPE
from xml.etree import ElementTree

ROOT = '/var/lib/opennms/rrd/snmp'
RRDTOOL = '/usr/bin/rrdtool'

class DSRC(object):
    data = None

    def __init__(self, ds_elem):
        """Parse given Data Source element in RRD XML"""
        self.data = {}
        for child in ds_elem.getchildren():
            value = child.text.strip()
            if child.tag in ['name', 'type', 'minimal_heartbeat', 'min', 'max',
                    'last_ds', 'value', 'unknown_sec']:
                self.data[child.tag] = value

class DATABASE(object):
    values = None

    def __init__(self, db_elem):
        """Parse given DATABASE element in RRD XML"""
        self.values = []
        for child in db_elem.getchildren():
            if child.tag != 'row': continue
            for gchild in child.getchildren():
                if gchild.tag != 'v': continue
                self.values.append(gchild.text.strip())

class RRA(object):
    data   = None
    db     = None
    points = None

    def __init__(self, rra_elem):
        """Parse given RRA element in RRD XML"""
        self.data   = {}
        self.db     = None
        self.points = None
        for child in rra_elem.getchildren():
            value = child.text.strip()
            if child.tag in ['cf']:
                self.data[child.tag] = value
            elif child.tag in ['pdp_per_row']:
                self.data[child.tag] = int(value)
            elif child.tag == 'database':
                self.db = DATABASE(child)

    def compute_stats(self):
        """Compute min, max, mean, and standard deviation of dataset"""
        self.points = None
        ymin = None
        ymax = None
        ysum = None
        A = 0.0
        Q = 0.0
        k = 0
        for value in self.db.values:
            if value == 'NaN': continue
            k += 1
            sample = float(value)
            if ymin is None or sample < ymin: ymin = sample 
            if ymax is None or sample > ymax: ymax = sample
            if ysum is None: ysum = 0
            ysum += sample
            Anew = A + (sample - A) / k
            Qnew = Q + (sample - A) * (sample - Anew)
            A = Anew
            Q = Qnew

        self.points = k
        self.ymin   = ymin
        self.ymax   = ymax
        self.ysum   = ysum
        if k > 0:
            self.mean    = A
            self.std_dev = math.sqrt(Q / k)
        else:
            self.mean    = None
            self.std_dev = None

    def __repr__(self):
        """Return compact representation of object"""
        prefix = '%s(%d)' % (self.data['cf'], self.data['pdp_per_row'])
        if self.points is None:
            return '%s - NOT COMPUTED' % prefix
        elif self.points:
            return '%s,%d,%g,%g,%g,%g,%g' % (prefix, self.points, self.ymin,
                                self.ymax, self.ysum, self.mean, self.std_dev)
        else:
            return '%s,%d,%s,%s,%s,%s,%s' % (prefix, self.points, self.ymin,
                                self.ymax, self.ysum, self.mean, self.std_dev)

class RRDFile(object):
    root = None
    data = None
    dsrc = None
    rras = None

    def __init__(self, dsrc_path):
        """dump RRD file as XML into temp file for parsing"""
        try:
            (fd, tpath) = tempfile.mkstemp()
            command = [RRDTOOL, 'dump', dsrc_path]
            sp = Popen(command, stdout=fd)
            sp.communicate()
            self._parse_xml(tpath)
        except:
            raise
        finally:
            os.close(fd)
            os.unlink(tpath)

    def _parse_xml(self, xml_path):
        """Parse given RRD XML file"""
        et = ElementTree.parse(xml_path)
        self.root = et.getroot()
        self.data = {}
        self.dsrc = None
        self.rras = []
        for child in et.getroot().getchildren():
            value = child.text.strip()
            if child.tag in ['version', 'step', 'lastupdate']:
                self.data[child.tag] = value
            elif child.tag == 'ds':
                self.dsrc = DSRC(child)
            elif child.tag == 'rra':
                self.rras.append(RRA(child))

    def _print_item(self, elem, level=0):
        """Helper for __repr__()"""
        value = elem.text.strip()
        lines = ['%s%s = %s' % (' ' * level, elem.tag, value)]
        if elem.tag != 'database':
            for child in elem.getchildren():
                lines.append(self._print_item(child, level + 1))
        return '\n'.join(lines)

    def __repr__(self):
        """Return a reasonable string representation of this object"""
        return self._print_item(self.root)

if __name__ == '__main__':

    # find all nodes in the OpenNMS SNMP data area
    nodes = os.listdir(ROOT)
    for node in nodes:
        node_path = os.path.join(ROOT, node)
        if not os.path.isdir(node_path): continue

        # find all interfaces on a given node
        ifaces = os.listdir(node_path)
        for iface in ifaces:
            iface_path = os.path.join(node_path, iface)
            if not os.path.isdir(iface_path): continue

            # find all data sources on a given interface
            dsrcs = os.listdir(iface_path)
            for dsrc in dsrcs:
                if not dsrc in ['ifInOctets.rrd', 'ifOutOctets.rrd']: continue
                dsrc_path = os.path.join(iface_path, dsrc)
                print dsrc_path

                # create an object from the data source file and play with it
                try:
                    rrdfile = RRDFile(dsrc_path)
                    for rra in rrdfile.rras:
                        rra.compute_stats()
                        print ' ', rra
                except (KeyboardInterrupt, SystemExit):
                    # really stop if requested by user or sys.exit
                    sys.exit(sys.exc_info()[1])
                except:
                    print "Unexpected error:", sys.exc_info()
