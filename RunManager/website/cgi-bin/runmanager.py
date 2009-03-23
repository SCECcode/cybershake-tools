#!/usr/bin/env python

# Add RunManager modules to PYTHONPATH
import sys
sys.path.append('/home/scec-00/patrices/code/trunk/RunManager/')

import cgi
from HTMLLib import *
from RunManager import *

# Enable python stack trace output to HTML
import cgitb; cgitb.enable() 

# Constants
FILTERS = ["Active", "Completed", "Deleted", "New Sites", ]


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


def dispNewSites():
    global rm

    site_list = rm.getNewSites()
    if ((site_list == None) or (len(site_list) == 0)):
        print "Unable to display new sites<p>"
        return 1

    t = HTMLTable()
    t.addCaption("New Sites (%s)" % (DB_HOST))
    t.addActions({"Add":"doadd.py"})
    t.setSelection(True)

    # Wrap a form around this table that allows multiple sites to be selected
    print "<form action=\"doadd.py\" method=\"POST\" enctype=\"multipart/form-data\" name=\"multiaddform\">"
    print "<p>"
    print "Click the appropriate link to add a run for that site, or select a group then click "
    print "<input type=\"submit\" value=\"Group Add\">"
    print "<p>"
    t.display(site_list)
    print "</form>"

    print "<p>"
    
    return 0


def main():
    global rm

    page = HTMLPage()
    page.header()
    page.pageTitle()
    page.menu([[MAIN_PAGE, "Main"],])
    page.sectionTitle("View Runs")
            
    form = cgi.FieldStorage() # instantiate only once!
    if form.has_key("filter") and form["filter"].value != "":
        filter = form["filter"].value
    else:
        filter = FILTERS[0]

    # Display selection form
    print "<form action=\"%s\" method=\"POST\" enctype=\"multipart/form-data\" name=\"selectform\">" % \
          (MAIN_PAGE)
    print "<select name=\"filter\">"
    print "<option value=\"%s\">%s</option>" % (filter, filter)
    for f in FILTERS:
        if (f != filter):
            print "<option value=\"%s\">%s</option>" % (f, f)
    print "</select>"
    print "<input type=\"submit\" value=\"Refresh\">"
    print "</form><p>"

    master_run_list = []

    if (filter == FILTERS[0]):
        target_states = ACTIVE_STATES
        caption = "Active Runs (%s)" % (DB_HOST)
    elif (filter == FILTERS[1]):
        target_states = [DONE_STATE,]
        caption = "Completed Runs (%s)" % (DB_HOST)
    elif (filter == FILTERS[2]):
        target_states = [DELETED_STATE,]
        caption = "Deleted Runs (%s)" % (DB_HOST)
    else:
        dispNewSites()
        page.footer(True)
        return 0
    
    # Show the selected runs
    for s in target_states:
        run = Run()
        run.setStatus(s)
        run_list = rm.getRuns(run)
        if ((run_list != None) and (len(run_list) > 0)):
            master_run_list = master_run_list + run_list

    t = HTMLTable()
    t.addCaption(caption)
    if (filter == FILTERS[0]):
        t.addActions({"Edit":"doedit.py", "Delete":"dodelete.py",})
    elif (filter == FILTERS[1]):
        t.addActions({"Details":"dispcurves.py",})
    t.display(master_run_list)

    print "<p>"
    
    page.footer(True)

    return 0


def cleanup():
    return 0


if __name__ == '__main__':
    if (init() != 0):
        sys.exit(1)
    main()
    cleanup()
    sys.exit(0)
