#!/usr/bin/env python

# Add RunManager modules to PYTHONPATH
import sys
import params
sys.path.append(params.RM_DIR)

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


def dispModifyForm(run):
    
    print "<form action=\"modifyrun.py\" method=\"POST\" enctype=\"multipart/form-data\" name=\"modifyform\">"
    print "<input type=\"hidden\" name=\"run_id\" value=\"%d\">" % (run.getRunID())
    print "<input type=\"hidden\" name=\"site_id\" value=\"%d\">" % (run.getSite().getSiteID())
    print "<input type=\"hidden\" name=\"site\" value=\"%s\">" % (run.getSite().getShortName())

    print "<div style=\"background: %s\">" % ("Beige")    

    print "<table width=\"1400\" border=\"0\">"

    # Site
    print "<tr>"
    print "<td>Site:</td>"
    print "<td>%s (%d)</td>" % (run.getSite().getShortName(), run.getSite().getSiteID())
    print "</tr>"

    # ERF_ID
    print "<tr>"
    print "<td>ERF ID:</td>"
    print "<td>"
    print "<select name=\"erf_id\">"
    print "<option value=\"%d\">%d</option>" % (run.getERFID(), run.getERFID())
    for erf in erf_list:
        if (int(erf[0]) != run.getERFID()):
            print "<option value=\"%s\">%s</option>" % (erf[0], erf[0])
    print "</select>"
    print "</td>"
    print "</tr>"
        
    # SGT_Var_ID
    print "<tr>"
    print "<td>SGT Variation ID:</td>"
    print "<td>"
    print "<select name=\"sgt_var_id\">"
    print "<option value=\"%d\">%d</option>" % (run.getSGTVarID(), run.getSGTVarID())
    for sgt_var in sgt_var_list:
        if (int(sgt_var[0]) != run.getSGTVarID()):
            print "<option value=\"%s\">%s</option>" % (sgt_var[0], sgt_var[0])
    print "</select>"
    print "</td>"
    print "</tr>"
        
    # Rup_Var_ID
    print "<tr>"
    print "<td>Rup Variation ID:</td>"
    print "<td>"
    print "<select name=\"rup_var_id\">"
    print "<option value=\"%d\">%d</option>" % (run.getRupVarID(), run.getRupVarID())
    for rup_var in rup_var_list:
        if (int(rup_var[0]) != run.getRupVarID()):
            print "<option value=\"%s\">%s</option>" % (rup_var[0], rup_var[0])
    print "</select>"
    print "</td>"
    print "</tr>"

    # Status
    print "<tr>"
    print "<td>Status:</td>"
    print "<td>"
    for status in STATUS_STD[run.getStatus()]:
        if (run.getStatus() == status):
            print "<input type=\"radio\" name=\"status\" value=\"%s\" checked> %s<br>" % (status, status)
        else:
            print "<input type=\"radio\" name=\"status\" value=\"%s\"> %s<br>" % (status, status)
    print "</td>"
    print "</tr>"

    # Status Time
    print "<tr>"
    print "<td>Status Time:</td>"
    print "<td><input type=\"hidden\" name=\"status_time\" value=\"%s\">%s GMT</td>" % \
          (run.getStatusTime(), run.getStatusTime())
    print "</tr>"
    
    # SGT_Host
    print "<tr>"
    print "<td>SGT Host:</td>"
    print "<td>"
    print "<select name=\"sgt_host\">"
    print "<option value=\"%s\">%s</option>" % (run.getSGTHost(), run.getSGTHost())
    for host in HOST_LIST:
        if (run.getSGTHost() != host):
            print "<option value=\"%s\">%s</option>" % (host, host)
    print "</select>"
    print "</td>"
    print "</tr>"

    # PP_Host
    print "<tr>"
    print "<td>PP Host:</td>"
    print "<td>"
    print "<select name=\"pp_host\">"
    print "<option value=\"%s\">%s</option>" % (run.getPPHost(), run.getPPHost())
    for host in HOST_LIST:
        if (run.getPPHost() != host):
            print "<option value=\"%s\">%s</option>" % (host, host)
    print "</select>"
    print "</td>"
    print "</tr>"

    # Comment
    print "<tr>"
    print "<td>Comment:</td>"
    print "<td>"
    if (run.getComment() == None):
        print "<textarea cols=80 rows=3 name=\"comment\"></textarea>"
    else:
        print "<textarea cols=80 rows=3 name=\"comment\">%s</textarea>" % (run.getComment())
    print "</td>"
    print "</tr>"

    # Last_User
    print "<tr>"
    print "<td>Last User:</td>"
    print "<td>"
    last_user = os.environ.get("REMOTE_USER")
    if (last_user == None):
        print "<select name=\"last_user\">"
        print "<option value=\"%s\">%s</option>" % (run.getLastUser(), run.getLastUser())
        for user in USER_LIST:
            if (run.getLastUser() != user):
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
    if (run.getJobID() == None):
        print "<input type=\"text\" name=\"job_id\" size=\"16\" value=\"\">"
    else:
        print "<input type=\"text\" name=\"job_id\" size=\"16\" value=\"%s\">" % (run.getJobID())
    print "</td>"
    print "</tr>"                        

    # Submit_Dir
    print "<tr>"
    print "<td>Submit Dir:</td>"
    print "<td>"
    if (run.getSubmitDir() == None):
        print "<textarea cols=80 rows=3 name=\"submit_dir\"></textarea>"
    else:
        print "<textarea cols=80 rows=3 name=\"submit_dir\">%s</textarea>" % \
              (run.getSubmitDir())
    print "</td>"
    print "</tr>"

    # Notify_User
    print "<tr>"
    print "<td>Notify User:</td>"
    print "<td>"
    if (run.getNotifyUser() == None):
        print "<input type=\"text\" name=\"notify_user\" size=\"32\" value=\"\">"
    else:
        print "<input type=\"text\" name=\"notify_user\" size=\"32\" value=\"%s\">" % (run.getNotifyUser())
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

    form = cgi.FieldStorage() # instantiate only once!

    page = HTMLPage()
    page.header()
    page.pageTitle()
    page.menu([[MAIN_PAGE, "Main"],])
    page.sectionTitle("Edit Run")

    if form.has_key("key") and form["key"].value != "":
        run_id = form["key"].value
        run = rm.getRunByID(run_id)
        if (run != None):
 
            # Retrieve ERF_IDs
            erf_list = rm.getParamIDs("ERF");
            
            # Retrieve SGT_Variation_IDs
            sgt_var_list = rm.getParamIDs("SGT_VAR");
            
            # Retrieve Rup_Variation_IDs
            rup_var_list = rm.getParamIDs("RUP_VAR");
            
            # Present form to allow user modification
            dispModifyForm(run);

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
