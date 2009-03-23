#!/usr/bin/env python

# Add RunManager modules to PYTHONPATH
import sys
sys.path.append('/home/scec-00/patrices/code/trunk/RunManager/')

import os
import cgi
from HTMLLib import *
from RunManager import *

# Enable python stack trace output to HTML
import cgitb; cgitb.enable() 


# Globals
rm = None


def init():
    global rm

    rm = RunManager(readonly=True)
    if (not rm.isValid()):
        print "Unable to open database connection.<p>"
        return 1
    
    rm.useHTML(True)
    
    return 0


def displayDetails(stats):
    print "<div style=\"background: %s\">" % ("Beige")

    print "<table width=\"400\" border=\"0\">"

    # Site
    print "<tr>"
    print "<td>Site:</td>"
    print "<td>%s</td>" % (stats.getSiteName())
    print "</tr>"
    
    # ERF_ID
    print "<tr>"
    print "<td>ERF ID:</td>"
    print "<td>%d</td>" % (stats.getERFID())
    print "</tr>"

    # SGT_Var_ID
    print "<tr>"
    print "<td>SGT Var ID:</td>"
    print "<td>%d</td>" % (stats.getSGTVarID())
    print "</tr>"
    
    # Rup_Var_ID
    print "<tr>"
    print "<td>Rup Var ID:</td>"
    print "<td>%d</td>" % (stats.getRupVarID())
    print "</tr>"

    # Num_PSA
    print "<tr>"
    print "<td>Number PSAs:</td>"
    print "<td>%d</td>" % (stats.getNumPSAs())
    print "</tr>"

    # Num_Curves
    print "<tr>"
    print "<td>Number Curves:</td>"
    print "<td>%d</td>" % (stats.getNumCurves())
    print "</tr>"

    print "</table>"
    print "</div>"

    return 0


def displayCurves(site):
    # Construct paths
    src_dir = "%s%s/" % (CURVE_DIR, site)
    
    # Get list of curves from src dir
    try:
        files = os.listdir(src_dir)
    except:
        files = []

    # Isolate the .png files
    curves = {}
    if (len(files) > 0):
        # Display all .png files
        i = 0
        for f in files:
            if (f.find('.png') != -1):
                i = i + 1
                srcname = "%s%s" % (src_dir, f)
                escaped = cgi.escape(srcname, True)
                curves[srcname] = escaped

    if (len(curves.keys()) == 0):
        print "No curve images found for site %s.<p>" % (site)
    else:
        # Display table of curves
        print "<table class=\"image\" border=\"0\">"
        print "<center><caption>Comparison Curve Images</caption></center>"
        # Display images
        print "<tr>"
        for k in curves.keys():
            print "<td>"
            print "<img src=\"loadpng.py?img=%s\" width=512 height=512><p>" % (curves[k])
            print "</td>"
        print "</tr>"

        # Print captions
        print "<tr>"
        for k in curves.keys():
            print "<td style=\"font-size:60%\">"
            print "<center>%s</center>" % (curves[k])
            print "</td>"
        print "</tr>"
        print "</table>"
        print "<p>"

    return 0


def main():
    global rm
    
    page = HTMLPage()
    page.header()
    page.pageTitle()
    page.menu([["%s?filter=Completed" % MAIN_PAGE, "Main"],])
    page.sectionTitle("Details")

    form = cgi.FieldStorage() # instantiate only once!
    run_id = None
    if form.has_key("key") and form["key"].value != "":
        run_id = form["key"].value

    if (run_id != None):
        # Pull run record and get site name
        stats = rm.getRunStatsByID(run_id)
        if (stats == None):
            print "Run %s not found in DB.<p>" % (run_id)
        else:
            displayDetails(stats)
            print "<p>"
            displayCurves(stats.getSiteName())

    page.footer()
    
    return 0


def cleanup():
    return 0


if __name__ == '__main__':
    if (init() != 0):
        sys.exit(1)
    main()
    cleanup()
    sys.exit(0)
