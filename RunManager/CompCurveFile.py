#!/usr/bin/env python

# Imports
import time
import cgi
from Config import *


# Stores info on a comparison curve file
class CompCurveFile:
    srcname = ""
    escaped = ""
    site = ""
    run_id = 0
    period = ""
    date = 0

    def __init__(self, srcname):
        self.srcname = srcname
        self.escaped = cgi.escape(self.srcname, True)
        suffix = self.srcname.split(".", 1)
        if (len(suffix) == 2):
            tokens = suffix[0].split("_")
            if (len(tokens) == 8):
                self.site = tokens[0]
                self.run_id = int(tokens[2][3:])
                self.period = tokens[3] + "_" + tokens[4]
                self.date = int(time.mktime(time.strptime('%s-%s-%s' % \
                                                              (tokens[7], \
                                                                   tokens[5], \
                                                                   tokens[6]), \
                                                              '%Y-%m-%d')))

    def getRunID(self):
        return self.run_id

    def getDate(self):
        return self.date

    def getSiteName(self):
        return self.site

    def getEscaped(self):
        return self.escaped

    def getFilename(self):
        return self.srcname

    def getPeriod(self):
        return self.period


