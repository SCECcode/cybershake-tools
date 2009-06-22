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
FILTERS = ["Active", "Completed", "Deleted", "New Sites", "Maps and Plots",]


# Globals
rm = None
write_access = False
remote_user = None

def init():
    global rm
    global remote_user
    global write_access

    rm = RunManager(readonly=True)
    if (not rm.isValid()):
        print "Unable to open database connection.<p>"
        return 1
    
    rm.useHTML(True)

    # Get remote user id
    remote_user = os.environ.get("REMOTE_USER")
    if (remote_user in USER_LIST):
        write_access = True
    else:
        write_access = False
    return 0


def dispNewSites():
    global rm

    site_list = rm.getNewSites()
    if ((site_list == None) or (len(site_list) == 0)):
        print "Unable to display new sites<p>"
        return 1

    t = HTMLTable()
    t.addCaption("New Sites")
    if (write_access == True):
        action_list = []
        action_list.append(HTMLAction("Add", "doadd.py", None))
        t.addActionList(action_list)
        t.setSelection(True)

        # Wrap a form around this table that allows multiple sites to be selected
        print "<form action=\"doadd.py\" method=\"POST\" enctype=\"multipart/form-data\" name=\"multiaddform\">"
        print "<p>"
        print "Click the appropriate link to add a run for that site, or select a group then click "
        print "<input type=\"submit\" value=\"Group Add\">"
        print "<p>"
        t.display(site_list)
        print "</form>"

    else:
        t.display(site_list)

    print "<p>"
    
    return 0


def dispMaps():
    global rm

    img_files = [SCATTER_IMG, INTERPOLATED_ALL_IMG, INTERPOLATED_GRID_IMG,]
    img_names = ["Scatter Map", "Interpolated, All Sites", \
                 "Interpolated, Gridded",]

    img_links = []
    for img in img_files:
        link_str = "loadpng.py?img=%s" % (cgi.escape(img, True))
        # Original img size: width=1275 height=1188
        src_str = "<img src=\"loadpng.py?img=%s\" border=\"1\" width=\"638\" height=\"594\">" % (cgi.escape(img, True))
        img_links.append("<a href=\"%s\">%s</a>" % (link_str, src_str))
        
    img_times = []
    for img in img_files:
        img_times.append(time.strftime("%Y-%m-%d %H:%M:%S GMT", \
                                       time.gmtime(os.path.getmtime(img))))

    print "<h4>Static Maps</h4>"
    print "Combined CyberShake/Base Maps for various SA and percentile thresholds can be viewed at the <a href=\"http://scecdata.usc.edu/wiki/index.php?title=CyberShake_2009_Map_Table#Interpolated_Map_Combined_with_Base_Maps_.28each_map_requires_manual_update.29\">Wiki Static Maps</a> (login required)."

    print "<h4>Dynamic Maps (Continuously Updated)</h4>"
    print "<p>"
    print "Click the image for a larger view."
    print "<p>"
    print "<table>"

    print "<tr>"
    for i in range(0, len(img_files)):
        print "<th>%s</th>" % (img_names[i])
    print "</tr>"
    
    print "<tr>"
    for i in range(0, len(img_files)):
        print "<td>%s</td>" % (img_links[i])
    print "</tr>"

    print "<tr>"
    for i in range(0, len(img_files)):
        print "<td><center>%s</center></td>" % (img_times[i])
    print "</tr>"

    print "</table>"
    print "<p>"
    return 0


def main():
    global rm
    global write_access

    page = HTMLPage()
    page.header()
    page.pageTitle()
    page.menu([[MAIN_PAGE, "Main"],[WIKI_PAGE, "Wiki"],["../notes.html","Notes"]])
    page.sectionTitle("Viewer")
            
    form = cgi.FieldStorage() # instantiate only once!
    if form.has_key("filter") and form["filter"].value != "":
        filter = form["filter"].value
    else:
        filter = FILTERS[0]

    # Display selection form
    print "<form action=\"%s\" method=\"POST\" enctype=\"multipart/form-data\" name=\"selectform\">" % \
          (MAIN_PAGE)

    print "<div style=\"background: %s\">" % ("Beige")
    print "<table width=\"800\" border=\"0\">"
    print "<tr>"
    print "<td>View:</td>"
    print "<td>"
    print "<select name=\"filter\">"
    print "<option value=\"%s\">%s</option>" % (filter, filter)
    for f in FILTERS:
        if (f != filter):
            print "<option value=\"%s\">%s</option>" % (f, f)
    print "</select>"
    print "<input type=\"submit\" value=\"Refresh\">"
    print "</td>"
    print "</tr>"

    print "<tr>"
    print "<td>Database:</td>"
    print "<td>%s : %d</td>" % (DB_HOST, DB_PORT)
    print "</tr>"

    print "<tr>"
    print "<td>Access Permissions:</td>"
    if (write_access == True):
        print "<td>View/Add/Modify/Delete (%s)</td>" % (remote_user)
    else:
        print "<td>View Only (%s)</td>" % (remote_user)
    print "</tr>"
    
    print "</table>"
    print "</div>"
    
    print "</form><p>"

    master_run_list = []

    if (filter == FILTERS[0]):
        target_states = ACTIVE_STATES
        caption = "Active Runs"
    elif (filter == FILTERS[1]):
        target_states = [DONE_STATE,]
        caption = "Completed Runs"
    elif (filter == FILTERS[2]):
        target_states = [DELETED_STATE,]
        caption = "Deleted Runs"
    elif (filter == FILTERS[3]):
        dispNewSites()
        page.footer(True)
        return 0
    else:
        dispMaps()
        page.footer(True)
        return 0
    
    # Show the selected runs
    run_list = rm.getRunsByState(target_states)
    if ((run_list != None) and (len(run_list) > 0)):
        master_run_list = run_list

    t = HTMLTable()
    t.addCaption(caption)
    action_list = []
    if (filter == FILTERS[0]):
        action_list.append(HTMLAction("Details", "details.py", "filter=%s" % (FILTERS[0])))
        if (write_access == True):
            action_list.append(HTMLAction("Edit", "doedit.py", None))
            action_list.append(HTMLAction("Delete", "dodelete.py", None))
        t.addActionList(action_list)
        t.display(master_run_list)
    elif (filter == FILTERS[1]):
        action_list.append(HTMLAction("Details", "details.py", "filter=%s" % (FILTERS[1])))
        action_list.append(HTMLAction("Stats", "dispstats.py", None))
        t.addActionList(action_list)
        t.setSelection(True)
        # Wrap a form around this table that allows multiple sites to be selected
        print "<form action=\"compcurves.py\" method=\"POST\" enctype=\"multipart/form-data\" name=\"multicompform\">"
        print "<p>"
        print "Click the appropriate link to view the site's curves, or select a group then click "
        print "<input type=\"submit\" value=\"Group Compare\"> to perform a comparison."
        print "<p>"
        t.display(master_run_list)
        print "</form>"
    elif (filter == FILTERS[2]):
        action_list.append(HTMLAction("Details", "details.py", "filter=%s" % (FILTERS[2])))
        t.addActionList(action_list)
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
