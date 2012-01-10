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
    page.sectionTitle("Add Run")


    form = cgi.FieldStorage() # instantiate only once!

    # Populate run object with common values
    run = Run()
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
    if form.has_key("comment") and form["comment"].value != "":
        run.setComment(form["comment"].value)
    if form.has_key("last_user") and form["last_user"].value != "":
        run.setLastUser(form["last_user"].value)
    if form.has_key("job_id") and form["job_id"].value != "":
        run.setJobID(form["job_id"].value)
    if form.has_key("submit_dir") and form["submit_dir"].value != "":
        run.setSubmitDir(form["submit_dir"].value)
    if form.has_key("notify_user") and form["notify_user"].value != "":
        run.setNotifyUser(form["notify_user"].value)

    # Determine which sites to create run
    site_id_list = []
    site_name_list = []
    if form.has_key("site_id"):
        if  (type(form["site_id"]) != type([])):
            if (form["site_id"].value != ""):
                site_id_list = [form["site_id"].value,]
        else:
            site_id_list = form.getlist("site_id")
    if form.has_key("site"):
        if  (type(form["site"]) != type([])):
            if (form["site"].value != ""):
                site_name_list = [form["site"].value,]
        else:
            site_name_list = form.getlist("site")

    i = 0
    site_map = {}
    for site in site_id_list:
        site_map[site] = site_name_list[i]
        i = i + 1

    for id, name in site_map.items():
        site = Site()
        site.setSiteID(int(id))
        site.setShortName(name)
        run.setSite(site)

        # Create the run
        run_id = rm.createRun(run)
        if (run_id == None):
            print "Run insert failed for site %s. Performing rollback.<p>" % (run.getSite().getShortName())
            rm.rollbackTransaction()
            break
        else:
            print "Run successfully added for site %s.<p>" % (run.getSite().getShortName())
            
    # Commit change
    rm.commitTransaction()

    if (len(site_map.keys()) == 0):
        print "No sites specified.<p>"
        
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
