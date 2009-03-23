#!/usr/bin/env python

# Add RunManager modules to PYTHONPATH
import sys
sys.path.append('/home/scec-00/patrices/code/trunk/RunManager/')

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
    page.sectionTitle("Edit Run")

    form = cgi.FieldStorage() # instantiate only once!
    run = Run()
    if form.has_key("run_id") and form["run_id"].value != "":
        run.setRunID(int(form["run_id"].value))
    if form.has_key("site_id") and form["site_id"].value != "":
        run.setSiteID(int(form["site_id"].value))
    if form.has_key("site") and form["site"].value != "":
        run.setSiteName(form["site"].value)
    if form.has_key("erf_id") and form["erf_id"].value != "":
        run.setERFID(int(form["erf_id"].value))
    if form.has_key("sgt_var_id") and form["sgt_var_id"].value != "":
        run.setSGTVarID(int(form["sgt_var_id"].value))
    if form.has_key("rup_var_id") and form["rup_var_id"].value != "":
        run.setRupVarID(int(form["rup_var_id"].value))
    if form.has_key("status") and form["status"].value != "":
        run.setStatus(form["status"].value)
    if form.has_key("status_time") and form["status_time"].value != "":
        run.setStatusTime(form["status_time"].value)
    if form.has_key("sgt_host") and form["sgt_host"].value != "":
        run.setSGTHost(form["sgt_host"].value)
    if form.has_key("pp_host") and form["pp_host"].value != "":
        run.setPPHost(form["pp_host"].value)
    if form.has_key("comment"):
        run.setComment(form["comment"].value)
    if form.has_key("last_user") and form["last_user"].value != "":
        run.setLastUser(form["last_user"].value)
    if form.has_key("job_id"):
        run.setJobID(form["job_id"].value)
    if form.has_key("submit_dir"):
        run.setSubmitDir(form["submit_dir"].value)
    if form.has_key("notify_user"):
        run.setNotifyUser(form["notify_user"].value)

    # Update status time
    run.setStatusTimeCurrent()
    if (run.getStatus() in SGT_STATES):
        run.setSGTTimeCurrent()
    if (run.getStatus() in PP_STATES):
        run.setPPTimeCurrent()

    # NOTE: Should perform these steps to ensure DB consistency
    # 1) Parse orig_status_time from cgi params (this is status_time from original user query on this run)
    # 2) Start transaction with begin()
    # 3) Query DB for this run
    # 4) Compare queried status_time with user status_time
    # 5) If the same, perform update. commit.
    # 6) Else, display error. rollback().

    # Perform update
    if (rm.updateRun(run) != 0):
        print "Run update failed for site %s.<p>" % (run.getSiteName())
    else:
        # Commit changes
        rm.commitTransaction()
        print "Run successfully updated for site %s.<p>" % (run.getSiteName())

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
    if (main() != 0):
        pass
    else:
        pass
    cleanup()
    sys.exit(0)
