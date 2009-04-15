#!/usr/bin/env python

# Add RunManager modules to PYTHONPATH
import sys
sys.path.append('/home/scec-00/patrices/code/trunk/RunManager/')

import os
import cgi
from RunManager import *
from HTMLLib import *


# Globals
rm = None


# Enable python stack trace output to HTML
import cgitb; cgitb.enable() 


def init():
    global rm

    rm = RunManager(readonly=False)
    if (not rm.isValid()):
        print "Unable to open database connection.<p>"
        return 1
    
    rm.useHTML(True)
    
    return 0


def dispDetails(run):
    
    print "<div style=\"background: %s\">" % ("Beige")
    print "<table width=\"1400\" border=\"0\">"

    # Run
    print "<tr>"
    print "<td>Run:</td>"
    print "<td>%d</td>" % (run.getRunID())
    print "</tr>"

    # Site
    print "<tr>"
    print "<td>Site:</td>"
    print "<td>%s (%d)</td>" % (run.getSite().getShortName(), run.getSite().getSiteID())
    print "</tr>"

    # ERF_ID
    print "<tr>"
    print "<td>ERF ID:</td>"
    print "<td>"
    print "%d" % (run.getERFID())
    print "</td>"
    print "</tr>"
        
    # SGT_Var_ID
    print "<tr>"
    print "<td>SGT Variation ID:</td>"
    print "<td>"
    print "%d" % (run.getSGTVarID())
    print "</td>"
    print "</tr>"
        
    # Rup_Var_ID
    print "<tr>"
    print "<td>Rup Variation ID:</td>"
    print "<td>"
    print "%d" % (run.getRupVarID())
    print "</td>"
    print "</tr>"

    # Status
    print "<tr>"
    print "<td>Status:</td>"
    print "<td>"
    print "%s (%s GMT)" % (run.getStatus(), run.getStatusTime())
    print "</td>"
    print "</tr>"

    # SGT_Host
    print "<tr>"
    print "<td>SGT Host:</td>"
    print "<td>"
    print "%s (%s GMT)" % (run.getSGTHost(), run.getSGTTime())
    print "</td>"
    print "</tr>"

    # PP_Host
    print "<tr>"
    print "<td>PP Host:</td>"
    print "<td>"
    print "%s (%s GMT)" % (run.getPPHost(), run.getPPTime())
    print "</td>"
    print "</tr>"

    # Comment
    print "<tr>"
    print "<td>Comment:</td>"
    print "<td>"
    if (run.getComment() != ""):
        print "<textarea cols=80 rows=3 name=\"comment\" readonly=\"yes\">%s</textarea>" % \
              (str(run.getComment()))
    else:
        print "<textarea cols=80 rows=3 name=\"comment\" readonly=\"yes\">None</textarea>"
    print "</td>"
    print "</tr>"

    # Last_User
    print "<tr>"
    print "<td>Last User:</td>"
    print "<td>"
    print "%s" % (str(run.getLastUser()))
    print "</td>"
    print "</tr>"

    # Job_ID
    print "<tr>"
    print "<td>Job ID:</td>"
    print "<td>"
    if (run.getJobID() != ""):
        print "%s" % (str(run.getJobID()))
    else:
        print "None"
    print "</td>"
    print "</tr>"                        

    # Submit_Dir
    print "<tr>"
    print "<td>Submit Dir:</td>"
    print "<td>"
    if (run.getSubmitDir() != ""):
        print "<textarea cols=80 rows=3 name=\"submit_dir\" readonly=\"yes\">%s</textarea>" % \
              (str(run.getSubmitDir()))
    else:
        print "<textarea cols=80 rows=3 name=\"submit_dir\" readonly=\"yes\">None</textarea>"
    print "</td>"
    print "</tr>"

    # Notify_User
    print "<tr>"
    print "<td>Notify User:</td>"
    print "<td>"
    if (run.getNotifyUser() != ""):
        print "%s" % (str(run.getNotifyUser()))
    else:
        print "None"
    print "</td>"
    print "</tr>"
    
    print "</table>"
    print "</div>"
    print "<p>"
    
    return 0


def main():
    global rm

    form = cgi.FieldStorage() # instantiate only once!
    filter = None
    if form.has_key("filter") and form["filter"].value != "":
        filter = form["filter"].value

    page = HTMLPage()
    page.header()
    page.pageTitle()
    if (filter == None):
        page.menu([["%s?filter=%s" % (MAIN_PAGE, "Active"), "Main"],])
    else:
        page.menu([["%s?filter=%s" % (MAIN_PAGE, filter), "Main"],])
    page.sectionTitle("Details")

    if form.has_key("key") and form["key"].value != "":
        run_id = form["key"].value
        run = rm.getRunByID(run_id)
        if (run != None):
 
            # Present form to allow user modification
            dispDetails(run);

            page.footer()
            return 0
        
    else:
        print "No run ID supplied.<p>"

    page.addRedirect(MAIN_PAGE)
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
