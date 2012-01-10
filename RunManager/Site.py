#!/usr/bin/env python

# Imports
import sys
import os
import pwd
import time
from Config import *


class Site:
    site_id = None
    site_short = None
    site_long = None
    lat = None
    lon = None

    def __init__(self):
        self.site_id = None
        self.site_short = None
        self.site_long = None
        self.lat = None
        self.lon = None

    def copy(self, obj):
        self.site_id = obj.site_id
        self.site_short = obj.site_short
        self.site_long = obj.site_long
        self.lat = obj.lat
        self.lon = obj.lon

    #@staticmethod
    def formatHeader(self):
        headers = ["Site ID", "Site Name", "Lat", "Lon", "Desc"]
        return headers

    def formatData(self):
        data = [str(self.site_id), \
                    str(self.site_short), \
                    str(self.lat), \
                    str(self.lon), \
                    str(self.site_long),]
        return data

    def getSiteID(self):
        return self.site_id

    def setSiteID(self, site_id):
        if (site_id == None):
            self.site_id = site_id
        else:
            self.site_id = int(site_id)

    def getLongName(self):
        return self.site_long

    def setLongName(self, site_long):
        if (site_long == None):
            self.site_long = site_long
        else:
            self.site_long = str(site_long)

    def getShortName(self):
        return self.site_short

    def setShortName(self, site_short):
        if (site_short == None):
            self.site_short = site_short
        else:
            self.site_short = str(site_short)

    def getLatitude(self):
        return self.lat

    def setLatitude(self, lat):
        if (lat == None):
            self.lat = lat
        else:
            self.lat = float(lat)

    def getLongitude(self):
        return self.lon

    def setLongitude(self, lon):
        if (lon == None):
            self.lon = lon
        else:
            self.lon = float(lon)

