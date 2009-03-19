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
erf_list = []
sgt_var_list = []
rup_var_list = []

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


def dispAddForm(run):
    
    print "<form action=\"addrun.py\" method=\"POST\" enctype=\"multipart/form-data\" name=\"addform\">"
    print "<input type=\"hidden\" name=\"site_id\" value=\"%d\">" % (run.getSiteID())
    print "<input type=\"hidden\" name=\"site\" value=\"%s\">" % (run.getSiteName())

    print "<div style=\"background: %s\">" % ("Beige")

    print "<table width=\"800\" border=\"0\">"

    # Site
    print "<tr>"
    print "<td>Site:</td>"
    print "<td>%s</td>" % (run.getSiteName())
    print "</tr>"

    # ERF_ID
    print "<tr>"
    print "<td>ERF ID:</td>"
    print "<td>"
    print "<select name=\"erf_id\">"
    for erf in erf_list:
        print "<option value=\"%d\">%d</option>" % (erf[0], erf[0])
    print "</select>"
    print "</td>"
    print "</tr>"

    # SGT_Var_ID
    print "<tr>"
    print "<td>SGT Variation ID:</td>"
    print "<td>"
    print "<select name=\"sgt_var_id\">"
    for sgt_var in sgt_var_list:
        print "<option value=\"%d\">%d</option>" % (sgt_var[0], sgt_var[0])
    print "</select>"
    print "</td>"
    print "</tr>"

    # Rup_Var_ID
    print "<tr>"
    print "<td>Rup Variation ID:</td>"
    print "<td>"
    print "<select name=\"rup_var_id\">"
    for rup_var in rup_var_list:
        print "<option value=\"%d\">%d</option>" % (rup_var[0], rup_var[0])
    print "</select>"
    print "</td>"
    print "</tr>"

    # Status
    print "<tr>"
    print "<td>Status:</td>"
    print "<td>"
    print "<input type=\"radio\" name=\"status\" value=\"%s\" checked> %s<br>" % (run.getStatus(), run.getStatus())
    print "</td>"
    print "</tr>"

    # Status Time
    print "<tr>"
    print "<td>Status Time:</td>"
    print "<td><input type=\"hidden\" name=\"status_time\" value=\"%s\">%s</td>" % \
          (run.getStatusTime(), run.getStatusTime())
    print "</tr>"
    
    # SGT_Host
    print "<tr>"
    print "<td>SGT Host:</td>"
    print "<td>"
    print "<select name=\"sgt_host\">"
    for host in HOST_LIST:
        print "<option value=\"%s\">%s</option>" % (host, host)
    print "</select>"
    print "</td>"
    print "</tr>"

    # PP_Host
    print "<tr>"
    print "<td>PP Host:</td>"
    print "<td>"
    print "<select name=\"pp_host\">"
    for host in HOST_LIST:
        print "<option value=\"%s\">%s</option>" % (host, host)
    print "</select>"
    print "</td>"
    print "</tr>"

    # Comment
    print "<tr>"
    print "<td>Comment:</td>"
    print "<td>"
    print "<textarea cols=32 rows=2 name=\"comment\">%s</textarea>" % (run.getComment())
    print "</td>"
    print "</tr>"

    # Last_User
    print "<tr>"
    print "<td>Last User:</td>"
    print "<td>"
    last_user = os.environ.get("REMOTE_USER")
    if (last_user == None):        
        print "<select name=\"last_user\">"
        for user in USER_LIST:
            print "<option value=\"%s\">%s</option>" % (user, user)
            print "</select>"
    else:
        print "<input type=\"hidden\" name=\"last_user\" value=\"%s\">" % (last_user)
        print "%s" % (last_user)
    print "</td>"
    print "</tr>"

    # Job_ID
    print "<tr>"
    print "<td>Job ID:</td>"
    print "<td>"
    print "<input type=\"text\" name=\"job_id\" size=\"12\" value=\"\">"
    print "</td>"
    print "</tr>"

    # Submit_Dir
    print "<tr>"
    print "<td>Submit Dir:</td>"
    print "<td>"
    print "<textarea cols=32 rows=2 name=\"submit_dir\">%s</textarea>" % (run.getSubmitDir())
    print "</td>"
    print "</tr>"

    print "</table>"

    print "</div>"

    # Place buttons
    print "<br>"
    print "<br>"
    print "<input type=\"submit\" value=\"Save\">"
    print "<input type=\"reset\" value=\"Clear\">"
    print "<br>"
    print "</form>"
    
    return 0


def main():
    global rm
    global erf_list
    global sgt_var_list
    global rup_var_list
    
    page = HTMLPage()
    page.header()
    page.pageTitle()
    page.menu([["%s?filter=New Sites" % (MAIN_PAGE), "Main"], ])
    page.sectionTitle("Add Run")

    form = cgi.FieldStorage() # instantiate only once!
    if form.has_key("key") and form["key"].value != "":
        site_id = int(form["key"].value)
        
        # Retrieve this site
        site = rm.getSiteByID(site_id);
            
        # Retrieve ERF_IDs
        erf_list = rm.getParamIDs("ERF");
        
        # Retrieve SGT_Variation_IDs
        sgt_var_list = rm.getParamIDs("SGT_VAR");
        
        # Retrieve Rup_Variation_IDs
        rup_var_list = rm.getParamIDs("RUP_VAR");

        if ((site == None) or (len(erf_list) == 0) or (len(sgt_var_list) == 0) or (len(rup_var_list) == 0)):
            print "Unable to retrieve reference info from DB.<p>"
        else:
            # Populate run object
            run = Run()
            run.setSiteID(site_id)
            run.setSiteName(site)
            run.setERFID(erf_list[0][0])
            run.setSGTVarID(sgt_var_list[0][0])
            run.setRupVarID(rup_var_list[0][0])
            run.setStatus(START_STATE)
            run.setStatusTimeCurrent()
            
            # Present form to allow user modification
            dispAddForm(run);
            
            page.footer()
            return 0

    else:
        print "No status ID supplied.<p>"

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
