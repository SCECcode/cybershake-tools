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
    action_map = None
    selection = False
    
    def __init__(self):
        self.caption = None
        self.action_map = None
        self.selection = False

        
    def addCaption(self, caption):
        self.caption = caption
        return 0


    def addActions(self, action_map):
        self.action_map = action_map
        return 0


    def setSelection(self, flag):
        self.selection = flag

        
    def display(self, data_list):
        print "<table width=\"1400\" border=\"1\">"

        if (self.caption != None):
            print "<center><caption>%s</caption></center>" % (self.caption)

        if (len(data_list) >= 0):

            headers = []
            if (self.selection == True):
                headers.append("Select")
            headers = headers + data_list[0].formatHeader()
            
            # Display column headers
            print "<tr bgcolor=\"DarkSalmon\" bordercolor=\"Black\">"
            for h in headers:
                print "<th scope=\"col\">%s</th>" % (h)
            if (self.action_map != None):
                print "<th scope=\"col\">%s</th>" % ("Actions")           
            print "</tr>"

            # Display data
            rownum = 0
            for datum in data_list:
                if (rownum % 2 == 0):
                    print "<tr style=\"font-size:80%;background:Beige\">"
                else:
                    print "<tr style=\"font-size:80%\">"

                row = datum.formatData()

                if (self.selection == True):
                    print "<td><center><input type=checkbox name=\"key\" value=\"%s\"></center></td>" % (row[0])
                    
                for c in row:
                    if (c == ""):
                        print "<td>None</td>"
                    elif (len(c) > 40):
                        print "<td style=\"font-size:65%\">"
                        longstr = c
                        while (len(longstr) > 0):
                            substr = longstr[0:40]
                            print "%s<br>" % (substr)
                            longstr = longstr[40:]
                        print "</td>"
                    else:
                        print "<td>%s</td>" % (c)

                if (self.action_map != None):
                    print "<td><center>"
                    for label,url in self.action_map.items():
                        print "<a href=\"%s?key=%s\">%s</a>" % (url, row[0], label)
                    print "</center></td>"
                        
                print "</tr>"
                rownum = rownum + 1                

        print "</table>"
        return 0

