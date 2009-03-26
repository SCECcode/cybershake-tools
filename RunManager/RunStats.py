#!/usr/bin/env python

# Imports
import sys
import os
import pwd
import time
from Config import *
from Site import *
from Curve import *


class RunStats:
    run_id = None
    #site_id = None
    #site = None
    site_obj = None
    erf_id = None
    sgt_var_id = None
    rup_var_id = None
    num_psa = None
    num_curves = None
    curve_list = None


    def __init__(self):
        self.run_id = None
        self.site_obj = Site()
        self.erf_id = None
        self.sgt_var_id = None
        self.rup_var_id = None
        self.num_psa = None
        self.num_curves = None
        self.curve_list = None

    def copy(self, obj):
        self.run_id = obj.run_id
        self.site_obj = obj.site_obj
        self.erf_id = obj.erf_id
        self.sgt_var_id = obj.sgt_var_id
        self.rup_var_id = obj.rup_var_id
        self.num_psa = obj.num_psa
        self.num_curves = obj.num_curves
        self.curve_list = obj.curve_list


    #@staticmethod
    def formatHeader(self):
        headers = ["Run ID", "Site", "ERF ID", "SGT Var ID", "Rup Var ID", \
                       "Num PSA", "Num Curves",]
        return headers

    def formatData(self):
        if (self.site_obj != None):
            site_name = self.site_obj.getShortName()
            site_id = self.site_obj.getSiteID()
        else:
            site_name = None
            site_id = None

        data = [str(self.run_id), \
                    "%s (%s)" % (str(site_name), str(site_id)), \
                    str(self.erf_id), \
                    str(self.sgt_var_id), \
                    str(self.rup_var_id), \
                    str(self.num_psa), \
                    str(self.num_curves),]
        return data

    def getRunID(self):
        return self.run_id

    def setRunID(self, run_id):
        if (run_id == None):
            self.run_id = run_id
        else:
            self.run_id = int(run_id)

    def getSite(self):
        return self.site_obj

    def setSite(self, site_obj):
        self.site_obj = site_obj

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
        if (self.num_psa == None):
            return 0
        else:
            return self.num_psa

    def setNumPSAs(self, num_psa):
        self.num_psa = int(num_psa)

    def getNumCurves(self):
        if (self.curve_list == None):
            return 0
        else:
            return len(self.curve_list)

    #def setNumCurves(self, num_curves):
    #    self.num_curves = int(num_curves)

    def getCurveList(self):
        return self.curve_list

    def setCurveList(self, curve_list):
        self.curve_list = curve_list
            
    def dumpToScreen(self):
        if (self.site_obj != None):
            site_name = self.site_obj.getShortName()
            site_id = self.site_obj.getSiteID()
        else:
            site_name = None
            site_id = None

        print "Run ID:\t\t %s" % (str(self.run_id))
        print "Site:\t\t %s (id=%s)" % (str(site_name), str(site_id))
        print "Params:\t\t erf=%s sgt_var=%s rup_var=%s" % \
            (str(self.erf_id), str(self.sgt_var_id), str(self.rup_var_id))
        print "Stats:\t\t num_psa=%s num_curves=%s" % \
            (str(self.num_psa), str(self.getNumCurves()))
        return 0
                                   
