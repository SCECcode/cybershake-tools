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



def displayCurves(stats):

    site = stats.getSite().getShortName()
    
    # Find a valid curve directory
    files = []
    curves = []
    for dir in CURVE_DIRS:
        # Construct paths
        src_dir = "%s%s/" % (dir, site)

        # Get list of curves from src dir
        try:
            files = os.listdir(src_dir)
        except:
            files = []

        # Isolate the .png files for this run
        if (len(files) > 0):
            # Display all .png files
            i = 0
            for f in files:
                if (f.find('.png') != -1):
                    i = i + 1
                    srcname = "%s%s" % (src_dir, f)
                    curvepic = CompCurveFile(srcname)
                    if (curvepic.getRunID() == stats.getRunID()):
                        curves.append(curvepic)

    # Construct data for table
    header_list = []
    img_list = []
    file_list = []
    for c in stats.getCurveList():
        per_str = "SA_%dsec" % (int(round(c.getIMValue())))
        header_list.append(per_str)
        match = None
        for curvepic in curves:
            if ((per_str == curvepic.getPeriod()) and \
                    (curvepic.getRunID() == stats.getRunID())):
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
    t.addCaption("Run %d (%s) Curves" % (stats.getRunID(), stats.getSite().getShortName()))
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
    page.sectionTitle("Multi-run Comparison")

    form = cgi.FieldStorage() # instantiate only once!

    run_id_list = []
    if form.has_key("key"):
        if  (type(form["key"]) != type([])):
            if (form["key"].value != ""):
                run_id_list = [form["key"].value,]
        else:
            run_id_list = form.getlist("key")
                
    if (len(run_id_list) > 0):
        for run_id in run_id_list:

            #print "RunID is %s<p>" % (run_id)
            if (run_id != None):
                # Pull run record and get site name
                stats = rm.getRunStatsByID(int(run_id))
                if (stats == None):
                    print "Run %s not found in DB.<p>" % (run_id)
                else:
                    print "<p>"
                    displayCurves(stats)
    else:
        print "No runs were selected.<p>"

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
