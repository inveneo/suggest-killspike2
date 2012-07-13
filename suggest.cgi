#!/usr/bin/env python

import os
import re
import cgi
import cgitb
import urllib
from urlparse import urlparse, parse_qs
import subprocess as sp

import rrd_file

# locations particular to this system
KILLSPIKE2 = '/var/www/spikes/killspike2'
RRD_ROOT   = '/var/lib/opennms/rrd/snmp'

# supported files
INFILE  = 'ifInOctets.rrd'
OUTFILE = 'ifOutOctets.rrd'

def spike_data(rrd_path, seconds):

    if not os.path.isfile(rrd_path):
        print 'Sorry: cannot find %s' % rrd_path
        return None
    rrd = rrd_file.RRDFile(rrd_path)
    step = int(rrd.data['step'])
    ds = rrd.dsrc.data['name']

    # check all the RRAs for candidates
    kept_rra = []
    for rra in rrd.rras:

        # if it's not AVERAGE, we don't want
        cf = rra.data['cf']
        if cf not in ['AVERAGE']: continue

        # if it's too short a duration, we don't want
        pdp_per_row = int(rra.data['pdp_per_row'])
        buflen = len(rra.db.values)
        duration = step * pdp_per_row * buflen
        if duration < seconds: continue
        kept_rra.append((duration, rra))

    # pick finest-grained remaining RRA
    kept_rra.sort()
    (duration, rra) = kept_rra[0]
    #print '<p>Chose RRA %s(%d)</p>' % (rra.data['cf'], rra.data['pdp_per_row'])

    # find ultimate and penultimate values in the data
    ult    = 0.0
    penult = 0.0
    points = 0
    for value in rra.db.values:
        if value == 'NaN': continue
        points += 1
        value = float(value)
        if ult < value:
            penult = ult
            ult = value
        elif penult < value:
            penult = value

    # suggest trimming between these two values
    newmax = int((ult + penult) / 2.0 + 0.5)
    return (rrd_path, ds, points, ult, penult, newmax)

def parse_post_data():

    # parse the POST data
    form = cgi.FieldStorage()
    url = urllib.unquote(form['url'].value)
    query = urlparse(url).query
    qs_dict = parse_qs(query)

    # parse the query string dictionary
    resourceId   = qs_dict['resourceId'][0]
    reports      = qs_dict['reports'][0]
    relativetime = qs_dict['relativetime'][0]

    match = re.match('node\[(\w+)\]\.interfaceSnmp\[(\w+)\]', resourceId)
    node  = match.group(1)
    iface = match.group(2)

    if reports not in ['mib2.bits']:
        print 'Sorry: reports=%s is not yet supported' % reports
        return (url, node, iface, None)

    if relativetime not in ['lastday', 'lastweek', 'lastmonth', 'lastyear']:
        print 'Sorry: relativetime=%s is not yet supported' % relativetime
        return (url, node, iface, None)
    if relativetime == 'lastday':
        seconds = 60 * 60 * 24
    elif relativetime == 'lastweek':
        seconds = 60 * 60 * 24 * 7
    elif relativetime == 'lastmonth':
        seconds = 60 * 60 * 24 * 31
    elif relativetime == 'lastyear':
        seconds = 60 * 60 * 24 * 365

    return (url, node, iface, seconds)

def scan_rrd_files(node, iface, seconds):
    """scan two RRD files to come up with command suggestions"""

    rrd_dir = os.path.join(RRD_ROOT, node, iface)
    if not os.path.isdir(rrd_dir):
        print 'Sorry: cannot find dir %s' % rrd_dir
        return

    for filename in [INFILE, OUTFILE]:
        rrd_path = os.path.join(rrd_dir, filename)
        results = spike_data(rrd_path, seconds)
        if results is None: continue
        (rrd_path, ds, points, ult, penult, newmax) = results
        print '<p>%s has %d points, max value=%f, next highest=%f</p>' % \
                                                (filename, points, ult, penult)
        print '<p>To delete the tallest spike, the suggested command is</p>'
        print '<p><b>%s %s %d %s</b></p>' % (KILLSPIKE2, ds, newmax, rrd_path)

if __name__ == '__main__':

    # for some nice debugging
    cgitb.enable()

    # start HTTP response here
    print 'Content-type: text/html\n\n'
    print '<html>'
    print '<head>'
    print '<title>killspike2 suggestion</title>'
    print '</head>'
    print '<body>'
    print '<h3>killspike2 suggestion</h3>'

    (url, node, iface, seconds) = parse_post_data()
    if node and iface and seconds:
        scan_rrd_files(node, iface, seconds)

    # finish HTTP response
    print "<hr>"
    print "<form action='suggest.cgi' method='post'>"
    print "<input type='hidden' name='url' size='80' value='%s'>" % url
    print "OK, I did it: now please"
    print "<input type='submit' value='Recompute'>"
    print "<p>(you should see one less point, and lower peak values)</p>"
    print "</form>"
    print '<p>Look again at <a href="%s" target="_blank">my graph</a></p>' % \
            url
    print '<p><a href="suggest.html">Return to blank input form</a></p>'
    print '</body>'
    print '</html>'
