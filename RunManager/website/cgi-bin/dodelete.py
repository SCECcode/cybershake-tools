#!/usr/bin/env python

# Add RunManager modules to PYTHONPATH
import sys
sys.path.append('/home/scec-00/patrices/code/trunk/RunManager/')

import cgi
from HTMLLib import *

# Enable python stack trace output to HTML
import cgitb; cgitb.enable() 


# Globals


def init():
    return 0


def main():

    page = HTMLPage()
    page.header()
    page.pageTitle()
    page.menu([[MAIN_PAGE, "Main"],])
    page.sectionTitle("Delete Run")

    form = cgi.FieldStorage() # instantiate only once!

    if form.has_key("key") and form["key"].value != "":
        run_id = int(form["key"].value)

        print "Please confirm, delete run %d?" % (run_id)
        print "<p>"

        print "<table>"
        print "<tr>"
        print "<td>"
        print "<form action=\"%s\" method=\"POST\" enctype=\"multipart/form-data\" name=\"confirmform\">" % ("deleterun.py")
        print "<input type=\"hidden\" name=\"key\" value=\"%s\">" % (str(run_id))
        print "<input type=\"submit\" value=\"Yes\">"
        print "</form>"
        print "</td>"

        print "<td>"
        print "<form action=\"%s\" method=\"POST\" enctype=\"multipart/form-data\" name=\"abortform\">" % ("runmanager.py")
        #print "<input type=\"hidden\" name=\"key\" value=\"%s\">" % (str(run_id))
        print "<input type=\"submit\" value=\"No\">"
        print "</form>"
        print "</td>"
        
        print "</tr>"
        print "</table>"

    else:
        print "No status ID supplied.<p>"
        print "If page does not redirect in a few seconds, please click the link above.<p>"
        print "Please wait...<p>"
        page.addRedirect(MAIN_PAGE)

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
