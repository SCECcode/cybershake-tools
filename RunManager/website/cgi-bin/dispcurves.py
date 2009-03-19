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


def main():
    global rm
    
    page = HTMLPage()
    page.header()
    page.pageTitle()
    page.menu([["%s?filter=Completed" % MAIN_PAGE, "Main"],])
    page.sectionTitle("Hazard Curves")

    form = cgi.FieldStorage() # instantiate only once!
    run_id = None
    site = None
    if form.has_key("key") and form["key"].value != "":
        run_id = form["key"].value

    if (run_id != None):
        # Pull run record and get site name
        run = rm.getRunByID(run_id)
        if (run == None):
            print "Run %s not found in DB.<p>" % (run_id)
        else:
            site = run.getSiteName()

    if (site != None):
            
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
