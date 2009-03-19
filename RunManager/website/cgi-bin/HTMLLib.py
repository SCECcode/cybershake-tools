#!/usr/bin/env python

import cgi

# Enable python stack trace output to HTML
import cgitb; cgitb.enable() 

# Constants

# Main web page
MAIN_PAGE = "runmanager.py"


class HTMLPage:
    def header(self, contentType = 'text/html'):
        print "Content-Type: %s\n" % (contentType)
        print "<html>"
        print "<head>"
        # Set params here
        print "<title>CyberShake Run Manager</title>"
        print "</head>"
        print "<body>"
        return 0


    def addRedirect(self, redirect):
        print "<meta http-equiv=\"refresh\" content=\"3; URL=%s\">" % (redirect)
        return 0


    def pageTitle(self):
        print "<h2>CyberShake Run Manager</h2>"
        #print "<hr>"                 
        return 0


    def menu(self, options):
        print "<p>Options: "
        for o in options:
            print "<A HREF=\"%s\">%s</A> " % (o[0], o[1])
        print "<p><hr>"
        return 0


    def sectionTitle(self, title):
        print "<p><h3>%s</h3></p>" % (title)
        return 0
        
    
    def footer(self, show_details = True):
        if (show_details):
            print "<hr>"
            print "&#169; 2009 <a href=\"http://www.scec.org\">Southern California Earthquake Center</a>, USC<br>"
            print "Maintained by <a href=\"mailto:patrices@usc.edu\">Patrick Small</a><p>"
        print "</body>"
        print "</html>"            
        return 0


class HTMLTable:
    caption = None
    headers = []
    data = []
    edit = None
    delete = None
    add = None
    
    def __init__(self, headers, data):
        self.headers = headers
        self.data = data
        self.edit = None
        self.delete = None
        self.add = None
        self.caption = None

    def _displayButtons(self, key_value):
        print "<center>"
        if (self.edit != None):
            print "<a href=\"%s?key=%s\">Edit</a>" % (self.edit, str(key_value))
        if (self.delete != None):
            print "<a href=\"%s?key=%s\">Delete</a>" % (self.delete, str(key_value))            
        if (self.add != None):
            print "<a href=\"%s?key=%s\">Add</a>" % (self.add, str(key_value))
        print "</center>"
        
        return 0


    def addCaption(self, caption):
        self.caption = caption
        return 0

    
    def display(self):
        print "<table width=\"1400\" border=\"1\">"

        if (self.caption != None):
            print "<center><caption>%s</caption></center>" % (self.caption)

        # Display column headers
        print "<tr bgcolor=\"DarkSalmon\" bordercolor=\"Black\">"
        for h in self.headers:
            print "<th scope=\"col\">%s</th>" % (h)
        if ((self.edit != None) or (self.delete != None) or (self.add != None)):
            print "<th scope=\"col\">%s</th>" % ("Actions")           
        print "</tr>"

        rownum = 0

        # Display data
        for row in self.data:
            if (rownum % 2 == 0):
                print "<tr style=\"font-size:80%;background:Beige\">"
            else:
                print "<tr style=\"font-size:80%\">"
            for col in row:
                print "<td>%s</td>" % (col)
            if ((self.edit != None) or (self.delete!= None) or (self.add != None)):
                # Assume first column is the key
                print "<td>"
                self._displayButtons(row[0])
                print "</td>"
            print "</tr>"
            rownum = rownum + 1

        print "</table>"
        return 0


    def showEdit(self, url):
        self.edit = url
        return 0


    def showDelete(self, url):
        self.delete = url
        return 0


    def showAdd(self, url):
        self.add = url
        return 0



class HTMLTableRuns:
    caption = None
    edit = None
    delete = None
    add = None
    curves = None
    
    def __init__(self):
        self.edit = None
        self.delete = None
        self.add = None
        self.curves = None
        self.caption = None
        
    def _displayButtons(self, row):
        print "<center>"
        if (self.edit != None):
            print "<a href=\"%s?key=%s\">Edit</a>" % (self.edit, str(row.getRunID()))
        if (self.delete != None):
            print "<a href=\"%s?key=%s\">Delete</a>" % (self.delete, str(row.getRunID()))            
        if (self.add != None):
            print "<a href=\"%s?key=%s\">Add</a>" % (self.add, str(row.getRunID()))
        if (self.curves != None):
            print "<a href=\"%s?key=%s\">Curves</a>" % (self.curves, str(row.getRunID()))
        print "</center>"
        
        return 0


    def addCaption(self, caption):
        self.caption = caption
        return 0

    
    def display(self, run_list):
        print "<table width=\"1400\" border=\"1\">"

        if (self.caption != None):
            print "<center><caption>%s</caption></center>" % (self.caption)

        headers = ["Run ID", "Site", "Status", "Status Time", "SGT Host", "PP Host", \
                   "Comment", "Last User", "Job ID", "Submit Dir", ]

        # Display column headers
        print "<tr bgcolor=\"DarkSalmon\" bordercolor=\"Black\">"
        for h in headers:
            print "<th scope=\"col\">%s</th>" % (h)
        if ((self.edit != None) or (self.delete != None) or (self.add != None) or (self.curves != None)):
            print "<th scope=\"col\">%s</th>" % ("Actions")           
        print "</tr>"

        # Display data
        rownum = 0
        for row in run_list:
            if (rownum % 2 == 0):
                print "<tr style=\"font-size:80%;background:Beige\">"
            else:
                print "<tr style=\"font-size:80%\">"
            
            print "<td>%s</td>" % (str(row.getRunID()))
            print "<td>%s (%s)</td>" % (str(row.getSiteName()), str(row.getSiteID()))
            print "<td>%s</td>" % (str(row.getStatus()))
            print "<td>%s</td>" % (str(row.getStatusTime()))
            print "<td>%s</td>" % (str(row.getSGTHost()))
            print "<td>%s</td>" % (str(row.getPPHost()))
            if ((row.getComment() != "") and (row.getComment() != None)):
                print "<td>%s</td>" % (str(row.getComment()))
            else:
                print "<td>None</td>"
            print "<td>%s</td>" % (str(row.getLastUser()))
            if ((row.getJobID() != "") and (row.getJobID() != None)):
                print "<td>%s</td>" % (str(row.getJobID()))
            else:
                print "<td>None</td>"
            if ((row.getSubmitDir() != "") and (row.getSubmitDir() != None)):
                print "<td style=\"font-size:65%\">"
                #print "<td>"
                submit_str = row.getSubmitDir()
                while (len(submit_str) > 0):
                    substr = submit_str[0:40]
                    print "%s<br>" % (substr)
                    submit_str = submit_str[40:]
                print "</td>"
            else:
                print "<td>None</td>"
                
            if ((self.edit != None) or (self.delete!= None) or (self.add != None) or (self.curves != None)):
                print "<td>"
                self._displayButtons(row)
                print "</td>"

            print "</tr>"
            rownum = rownum + 1

        print "</table>"
        return 0


    def showEdit(self, url):
        self.edit = url
        return 0


    def showDelete(self, url):
        self.delete = url
        return 0


    def showAdd(self, url):
        self.add = url
        return 0


    def showCurves(self, url):
        self.curves = url
        return 0



class HTMLTableSites:
    caption = None
    edit = None
    delete = None
    add = None
    
    def __init__(self):
        self.edit = None
        self.delete = None
        self.add = None
        self.caption = None
        
    def _displayButtons(self, row):
        print "<center>"
        if (self.edit != None):
            print "<a href=\"%s?key=%s\">Edit</a>" % (self.edit, str(row.getSiteID()))
        if (self.delete != None):
            print "<a href=\"%s?key=%s\">Delete</a>" % (self.delete, str(row.getSiteID()))            
        if (self.add != None):
            print "<a href=\"%s?key=%s\">Add</a>" % (self.add, str(row.getSiteID()))
        print "</center>"
        
        return 0


    def addCaption(self, caption):
        self.caption = caption
        return 0

    
    def display(self, site_list):
        print "<table width=\"1400\" border=\"1\">"

        if (self.caption != None):
            print "<center><caption>%s</caption></center>" % (self.caption)

        headers = ["Site ID", "Site Name", "Lat", "Lon", "Desc"]
        
        # Display column headers
        print "<tr bgcolor=\"DarkSalmon\" bordercolor=\"Black\">"
        for h in headers:
            print "<th scope=\"col\">%s</th>" % (h)
        if ((self.edit != None) or (self.delete != None) or (self.add != None)):
            print "<th scope=\"col\">%s</th>" % ("Actions")           
        print "</tr>"

        # Display data
        rownum = 0
        for row in site_list:
            if (rownum % 2 == 0):
                print "<tr style=\"font-size:80%;background:Beige\">"
            else:
                print "<tr style=\"font-size:80%\">"
            
            print "<td>%s</td>" % (str(row.getSiteID()))
            print "<td>%s</td>" % (str(row.getShortName()))
            print "<td>%s</td>" % (str(row.getLatitude()))
            print "<td>%s</td>" % (str(row.getLongitude()))
            print "<td>%s</td>" % (str(row.getLongName()))
            
            if ((self.edit != None) or (self.delete!= None) or (self.add != None)):
                print "<td>"
                self._displayButtons(row)
                print "</td>"

            print "</tr>"
            rownum = rownum + 1

        print "</table>"
        return 0


    def showEdit(self, url):
        self.edit = url
        return 0


    def showDelete(self, url):
        self.delete = url
        return 0


    def showAdd(self, url):
        self.add = url
        return 0

