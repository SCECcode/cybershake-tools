#!/usr/bin/env python

# Add RunManager modules to PYTHONPATH
import sys
sys.path.append('/home/scec-00/patrices/code/trunk/RunManager/')

import os
import cgi
from RunManager import *
from HTMLLib import *

# Enable python stack trace output to HTML
import cgitb; cgitb.enable() 


# Globals
rm = None


def init():
    global rm

    rm = RunManager(readonly=False)
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
    page.menu([[MAIN_PAGE, "Main"],])
    page.sectionTitle("Delete Run")

    # Get the user_id if available
    last_user = os.environ.get("REMOTE_USER")
    
    form = cgi.FieldStorage() # instantiate only once!
    if form.has_key("key") and form["key"].value != "":
        run_id = int(form["key"].value)
        if (rm.deleteRunByID(run_id, last_user) != 0):
            print "Unable to delete run %s<p>" % (run_id)
        else:
            # Commit change
            rm.commitTransaction()
            print "Run successfully deleted from database.<p>"            
    else:
        print "No status ID supplied.<p>"

    print "If page does not redirect in a few seconds, please click the link above.<p>"
    print "Please wait...<p>"

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
