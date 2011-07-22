#!/usr/bin/env python

# Add RunManager modules to PYTHONPATH
import sys
import params
sys.path.append(params.RM_DIR)

import os
import cgi
from HTMLLib import *
from RunManager import *
from CompCurveFile import *

# Enable python stack trace output to HTML
import cgitb; cgitb.enable() 

# Constants
NO_IMG_TXT = "<table width=600><tr><td><center>[Image not found]</center></td></tr></table>"
NO_FILE_TXT = "<center>Unknown</center>"


# Globals
rm = None

class Data:
    headers = []
    vals = []
    
    def __init__(self, headers, vals):
        self.headers = headers
        self.vals = vals

    def formatHeader(self):
        return self.headers

    def formatData(self):
        return self.vals


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

    print "<table width=\"800\" border=\"0\">"

    site = stats.getSite()

    # Run
    print "<tr>"
    print "<td>Run:</td>"
    print "<td>%d</td>" % (stats.getRunID())
    print "</tr>"
    
    # Site
    print "<tr>"
    print "<td>Site:</td>"
    print "<td>%s (%d), %s</td>" % (site.getShortName(), site.getSiteID(), site.getLongName())
    print "</tr>"

    # Location
    print "<tr>"
    print "<td>Location:</td>"
    print "<td>lat=%f, lon=%f</td>" % (site.getLatitude(), site.getLongitude())
    print "</tr>"

    # Site Type
    print "<tr>"
    print "<td>Type:</td>"
    print "<td>%s</td>" % (site.getSiteType())
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


def displayCurves(stats):

    site = stats.getSite().getShortName()
    
    # Construct paths
    src_dir = "%s%s/" % (CURVE_DIR, site)
    
    # Get list of curves from src dir
    try:
        files = os.listdir(src_dir)
    except:
        files = []

    # Isolate the .png files
    curves = []
    if (len(files) > 0):
        # Display all .png files
        i = 0
        for f in files:
            if (f.find('.png') != -1):
                i = i + 1
                srcname = "%s%s" % (src_dir, f)
                curvepic = CompCurveFile(srcname)
                curves.append(curvepic)

    # Construct data for table
    header_list = []
    img_list = []
    file_list = []
    for c in stats.getCurveList():
        per_str = "SA_%dsec" % (int(round(c.getIMValue())))
        header_list.append(per_str)
        found = False
        match = None
        for curvepic in curves:
            if ((per_str == curvepic.getPeriod()) and (curvepic.getRunID() == stats.getRunID())):
                if ((match == None) or (match.getDate() < curvepic.getDate())):
                    match = curvepic

        if (match != None):
            html_str = "<img src=\"loadpng.py?img=%s\">" % (match.getEscaped())
            file_str = "<center>%s</center>" % (match.getFilename())
            img_list.append(html_str)
            file_list.append(file_str)
        else:
            img_list.append(NO_IMG_TXT)
            file_list.append(NO_FILE_TXT)

    data_list = []
    data_list.append(Data(header_list, img_list))
    data_list.append(Data(header_list, file_list))

    # Display the table
    t = HTMLTable()
    t.setWidth(None)
    t.addCaption("Comparison Curves")
    t.allowWrap(False)
    t.display(data_list)

    print "<p>"

    return 0


def main():
    global rm
    
    page = HTMLPage()
    page.header()
    page.pageTitle()
    page.menu([["%s?filter=Completed" % MAIN_PAGE, "Main"],])
    page.sectionTitle("Run Stats/Curves")

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
            displayCurves(stats)

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
