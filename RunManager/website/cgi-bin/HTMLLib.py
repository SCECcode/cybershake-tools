#!/usr/bin/env python

import cgi

# Enable python stack trace output to HTML
import cgitb; cgitb.enable() 

# Constants

# Main web page
MAIN_PAGE = "runmanager.py"
WIKI_PAGE = "http://scecdata.usc.edu/wiki/index.php?title=CyberShake_2009_Production_Runs"


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


class HTMLAction:
    label = None
    url = None
    arg_list = None

    def __init__(self, label, url, arg_list):
        self.label = label
        self.url = url
        self.arg_list = arg_list

    def getLabel(self):
        return self.label
    
    def getURL(self):
        return self.url

    def getArgList(self):
        return self.arg_list
    

class HTMLTable:
    caption = None
    action_list = None
    selection = False
    allow_wrap = True
    width = None
    
    def __init__(self):
        self.caption = None
        self.action_list = None
        self.selection = False
        self.allow_wrap = True
        self.width = 1400
        self.max_col_width = 60


    def __splitString(self, str):
        string_list = []

        has_space = str.find(" ")
        has_slash = str.find("/")

        if (has_space != -1):
            sep = " "
        elif (has_slash != -1):
            sep = "/"
        else:
            longstr = str
            while (len(longstr) > self.max_col_width):
                string_list.append(longstr[0:self.max_col_width])
                longstr = longstr[self.max_col_width:]
            if (len(longstr) > 0):
                string_list.append(longstr)
            return string_list
        
        line = ""
        tokens = str.split(sep)
        i = 0
        for t in tokens:
            # If line will be excessively long, write out current line
            if (len(line) + len(t) > self.max_col_width + 5):
                string_list.append(line)
                line = ""
            # Only write separator for tokens 2-N
            if ((i == 0) and (len(t) == 0)):
                pass
            else:
                line = line + sep + t
            # If line exceeds max size, write out current line
            if (len(line) > self.max_col_width):
                string_list.append(line)
                line = ""
            i = i + 1

        if (line != ""):
            string_list.append(line)
        return string_list
    
        
    def addCaption(self, caption):
        self.caption = caption
        return 0


    def addActionList(self, action_list):
        self.action_list = action_list
        return 0


    def setSelection(self, flag):
        self.selection = flag


    def allowWrap(self, flag):
        self.allow_wrap = flag
        

    def setWidth(self, width):
        self.width = width

        
    def display(self, data_list):
        if (self.width != None):
            print "<table width=\"1400\" border=\"1\">"
        else:
            print "<table border=\"1\">"

        if (self.caption != None):
            print "<center><caption>%s</caption></center>" % (self.caption)

        if (len(data_list) > 0):

            headers = []
            if (self.selection == True):
                headers.append("Select")
            headers = headers + data_list[0].formatHeader()
            
            # Display column headers
            print "<tr bgcolor=\"DarkSalmon\" bordercolor=\"Black\">"
            for h in headers:
                print "<th scope=\"col\">%s</th>" % (h)
            if (self.action_list != None):
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
                    elif ((self.allow_wrap == True) and (len(c) > self.max_col_width) and (c[0] != '<')):
                        print "<td style=\"font-size:100%\">"
                        string_list = self.__splitString(c)
                        for substr in string_list:
                            print "%s<br>" % (substr)
                        print "</td>"
                    else:
                        print "<td>%s</td>" % (c)

                if (self.action_list != None):
                    print "<td><center>"
                    for action in self.action_list:
                        if (action.getArgList() == None):
                            argstr = ""
                        else:
                            argstr = "&" + action.getArgList()
                        print "<a href=\"%s?key=%s%s\">%s</a>" % \
                              (action.getURL(), row[0], argstr, action.getLabel())
                    print "</center></td>"

                print "</tr>"
                rownum = rownum + 1                

        else:
            print "<tr><td><center>No data</center></td></tr>"

        print "</table>"
        return 0

