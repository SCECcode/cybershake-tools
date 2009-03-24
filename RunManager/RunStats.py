#!/usr/bin/env python

# Imports
import sys
import os
import pwd
import time
from Config import *


class RunStats:
    run_id = None
    site_id = None
    site = None
    erf_id = None
    sgt_var_id = None
    rup_var_id = None
    num_psa = None
    num_curves = None


    def __init__(self):
        self.run_id = None
        self.site_id = None
        self.site = None
        self.erf_id = None
        self.sgt_var_id = None
        self.rup_var_id = None
        self.num_psa = None
        self.num_curves = None

    def copy(self, obj):
        self.run_id = obj.run_id
        self.site_id = obj.site_id
        self.site = obj.site
        self.erf_id = obj.erf_id
        self.sgt_var_id = obj.sgt_var_id
        self.rup_var_id = obj.rup_var_id
        self.num_psa = obj.num_psa
        self.num_curves = obj.num_curves


    #@staticmethod
    def formatHeader(self):
        headers = ["Run ID", "Site", "ERF ID", "SGT Var ID", "Rup Var ID", \
                       "Num PSA", "Num Curves",]
        return headers

    def formatData(self):
        data = [str(self.run_id), \
                    "%s (%s)" % (str(self.site), str(self.site_id)), \
                    str(self.erf_id), \
                    str(self.sgt_var_id), \
                    str(self.rup_var_id), \
                    str(self.num_psa), \
                    str(self.num_curves),]
        return data

    def getRunID(self):
        return self.run_id

    def setRunID(self, run_id):
        # Allow None as a valid run_id
        if (run_id == None):
            self.run_id = run_id
        else:
            self.run_id = int(run_id)

    def getSiteID(self):
        return self.site_id

    def setSiteID(self, site_id):
        if (site_id == None):
            self.site_id = site_id
        else:
            self.site_id = int(site_id)

    def getSiteName(self):
        return self.site

    def setSiteName(self, site):
        if (site == None):
            self.site = site
        else:
            self.site = str(site)

    def getERFID(self):
        return self.erf_id

    def setERFID(self, erf_id):
        if (erf_id == None):
            self.erf_id = erf_id
        else:
            self.erf_id = int(erf_id)

    def getSGTVarID(self):
        return self.sgt_var_id

    def setSGTVarID(self, sgt_var_id):
        if (sgt_var_id == None):
            self.sgt_var_id = sgt_var_id
        else:
            self.sgt_var_id = int(sgt_var_id)

    def getRupVarID(self):
        return self.rup_var_id

    def setRupVarID(self, rup_var_id):
        if (rup_var_id == None):
            self.rup_var_id = rup_var_id
        else:
            self.rup_var_id = int(rup_var_id)

    def getNumPSAs(self):
        return self.num_psa

    def setNumPSAs(self, num_psa):
        self.num_psa = num_psa

    def getNumCurves(self):
        return self.num_curves

    def setNumCurves(self, num_curves):
        self.num_curves = num_curves

            
    def dumpToScreen(self):
        print "Run ID:\t\t %s" % (str(self.run_id))
        print "Site:\t\t %s (id=%s)" % (str(self.site), str(self.site_id))
        print "Params:\t\t erf=%s sgt_var=%s rup_var=%s" % \
            (str(self.erf_id), str(self.sgt_var_id), str(self.rup_var_id))
        print "Stats:\t\t num_psa=%s num_curves=%s" % \
            (str(self.num_psa), str(self.num_curves))
        return 0
                                                                            
