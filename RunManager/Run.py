#!/usr/bin/env python

# Imports
import sys
import os
import pwd
import time
from Config import *


class Run:
    run_id = None
    site_id = None
    site = None
    erf_id = None
    sgt_var_id = None
    rup_var_id = None
    status = None
    status_time = None
    sgt_host = None
    sgt_time = None
    pp_host = None
    pp_time = None
    comment = None
    last_user = None
    job_id = None
    submit_dir = None


    def __init__(self):
        self.run_id = None
        self.site_id = None
        self.site = None
        self.erf_id = None
        self.sgt_var_id = None
        self.rup_var_id = None
        self.status = None
        self.status_time = None
        self.sgt_host = None
        self.sgt_time = None
        self.pp_host = None
        self.pp_time = None
        self.comment = None
        self.last_user = None
        self.job_id = None
        self.submit_dir = None


    def copy(self, obj):
        self.run_id = obj.run_id
        self.site_id = obj.site_id
        self.site = obj.site
        self.erf_id = obj.erf_id
        self.sgt_var_id = obj.sgt_var_id
        self.rup_var_id = obj.rup_var_id
        self.status = obj.status
        self.status_time = obj.status_time
        self.sgt_host = obj.sgt_host
        self.sgt_time = obj.sgt_time
        self.pp_host = obj.pp_host
        self.pp_time = obj.pp_time
        self.comment = obj.comment
        self.last_user = obj.last_user
        self.job_id = obj.job_id
        self.submit_dir = obj.submit_dir


    @staticmethod
    def formatHeader():
        headers = ["Run ID", "Site", "Status", "Status Time", "SGT Host", \
                       "PP Host", "Comment", "Last User", "Job ID", \
                       "Submit Dir", ]
        return headers

    def formatData(self):
        data = [str(self.run_id), \
                    "%s (%s)" % (str(self.site), str(self.site_id)), \
                    str(self.status), \
                    str(self.status_time), \
                    str(self.sgt_host), \
                    str(self.pp_host), \
                    str(self.comment), \
                    str(self.last_user), \
                    str(self.job_id), \
                    str(self.submit_dir),]
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

    def getStatus(self):
        return self.status

    def setStatus(self, status):
        if (status == None):
            self.status = status
        else:
            self.status = str(status)

    def getStatusTime(self):
        return self.status_time

    def setStatusTime(self, status_time):
        if (status_time == None):
            self.status_time = status_time
        else:
            self.status_time = str(status_time)

    def setStatusTimeCurrent(self):
        self.status_time = time.strftime("%Y-%m-%d %H:%M:%S")

    def getSGTHost(self):
        return self.sgt_host

    def setSGTHost(self, sgt_host):
        if (sgt_host == None):
            self.sgt_host = sgt_host
        else:
            self.sgt_host = str(sgt_host)

    def getSGTTime(self):
        return self.sgt_time

    def setSGTTime(self, sgt_time):
        if (sgt_time == None):
            self.sgt_time = sgt_time
        else:
            self.sgt_time = str(sgt_time)

    def setSGTTimeCurrent(self):
        self.sgt_time = time.strftime("%Y-%m-%d %H:%M:%S")

    def getPPHost(self):
        return self.pp_host

    def setPPHost(self, pp_host):
        if (pp_host == None):
            self.pp_host = pp_host
        else:
            self.pp_host = str(pp_host)

    def getPPTime(self):
        return self.pp_time

    def setPPTime(self, pp_time):
        if (pp_time == None):
            self.pp_time = pp_time
        else:
            self.pp_time = str(pp_time)

    def setPPTimeCurrent(self):
        self.pp_time = time.strftime("%Y-%m-%d %H:%M:%S")

    def getComment(self):
        return self.comment

    def setComment(self, comment):
        if (comment == None):
            self.comment = comment
        else:
            self.comment = str(comment)

    def getLastUser(self):
        return self.last_user

    def setLastUser(self, last_user):
        if (last_user == None):
            self.last_user = last_user
        else:
            self.last_user = str(last_user)

    def setLastUserCurrent(self):
        self.last_user = pwd.getpwuid(os.getuid())[0]
                    
    def getJobID(self):
        return self.job_id

    def setJobID(self, job_id):
        if (job_id == None):
            self.job_id = job_id
        else:
            self.job_id = str(job_id)

    def getSubmitDir(self):
        return self.submit_dir

    def setSubmitDir(self, submit_dir):
        if (submit_dir == None):
            self.submit_dir = submit_dir
        else:
            self.submit_dir = str(submit_dir)
            
    def dumpToScreen(self):
        print "Run ID:\t\t %s" % (str(self.run_id))
        print "Site:\t\t %s (id=%s)" % (str(self.site), str(self.site_id))
        print "Params:\t\t erf=%s sgt_var=%s rup_var=%s" % (str(self.erf_id), \
                                                                str(self.sgt_var_id), \
                                                                str(self.rup_var_id))
        print "State:\t\t state='%s' time='%s'" % (str(self.status), str(self.status_time))
        print "SGT Info:\t host=%s time='%s'" % (str(self.sgt_host), str(self.sgt_time))
        print "PP Info:\t host=%s time='%s'" % (str(self.pp_host), str(self.pp_time))
        print "Comment:\t '%s'" % (str(self.comment))
        print "Tracking:\t user='%s' job_id='%s'" % (str(self.last_user), str(self.job_id))
        print "Submit Dir:\t %s" % (str(self.submit_dir))
        return 0
                                                                            
