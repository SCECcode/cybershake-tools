#!/usr/bin/env python

# Imports
import sys
import os
import pwd
import time
from Config import *
from Site import *


class Run:
    run_id = None
    site_obj = None
    erf_id = None
    sgt_var_id = None
    vel_id = None
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
    notify_user = None
    max_freq = None
    low_freq_cutoff = None

    def __init__(self):
        self.run_id = None
        self.site_obj = Site()
        self.erf_id = None
        self.sgt_var_id = None
        self.vel_id = None
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
        self.notify_user = None
        self.max_freq = None
        self.low_freq_cutoff = None


    def copy(self, obj):
        self.run_id = obj.run_id
        self.site_obj = obj.site_obj
        self.erf_id = obj.erf_id
        self.sgt_var_id = obj.sgt_var_id
        self.vel_id = obj.vel_id
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
        self.notify_user = obj.notify_user
        self.max_freq = obj.max_freq
        self.low_freq_cutoff = obj.low_freq_cutoff


    #@staticmethod
    def formatHeader(self):
        headers = ["Run ID", "Site", "Status", "Status Time (GMT)", \
                       "SGT Host", "PP Host", "Comment", "Job ID", ]
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
                    str(self.status), \
                    str(self.status_time), \
                    str(self.sgt_host), \
                    str(self.pp_host), \
                    str(self.comment), \
                    str(self.job_id),]
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

    def setVelID(self, vel_id):
        self.vel_id = vel_id

    def getVelID(self):
        return(self.vel_id)

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
        self.status_time = time.strftime("%Y-%m-%d %H:%M:%S", \
                                             time.gmtime(time.time()))

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
        self.sgt_time = time.strftime("%Y-%m-%d %H:%M:%S", \
                                          time.gmtime(time.time()))

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
        self.pp_time = time.strftime("%Y-%m-%d %H:%M:%S", \
                                         time.gmtime(time.time()))

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
        #if (self.job_id == ""):
        #    return None
        return self.job_id

    def setJobID(self, job_id):
        if (job_id == None):
            self.job_id = job_id
        else:
            self.job_id = str(job_id)

    def getSubmitDir(self):
        #if (self.submit_dir == ""):
        #    return None
        return self.submit_dir

    def setSubmitDir(self, submit_dir):
        if (submit_dir == None):
            self.submit_dir = submit_dir
        else:
            self.submit_dir = str(submit_dir)

    def getNotifyUser(self):
        #if (self.notify_user == ""):
        #    return None
        return self.notify_user

    def getNotifyUserAsList(self):
        if ((self.notify_user == None) or (self.notify_user == "")):
            return None
        else:
            return self.notify_user.split(",")

    def setNotifyUser(self, notify_user):
        if (notify_user == None):
            self.notify_user = notify_user
        elif (type(notify_user) == type([])):
            self.notify_user = ""
            for n in notify_user:
                if (self.notify_user == ""):
                    self.notify_user = n
                else:
                    self.notify_user = self.notify_user + "," + n
        else:
            self.notify_user = str(notify_user)

    def getMaxFreq(self):
        return(self.max_freq)

    def setMaxFreq(self, max_freq):
        if (max_freq == None):
            self.max_freq = max_freq
        else:
            self.max_freq = str(max_freq)

    def getLowFreqCutoff(self):
        return(self.low_freq_cutoff)

    def setLowFreqCutoff(self, low_freq_cutoff):
        if (low_freq_cutoff == None):
            self.low_freq_cutoff = low_freq_cutoff
        else:
            self.low_freq_cutoff = str(low_freq_cutoff)

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
            (str(self.erf_id), \
                 str(self.sgt_var_id), \
                 str(self.rup_var_id))
        print "State:\t\t state='%s' time='%s'" % \
            (str(self.status), str(self.status_time))
        print "SGT Info:\t host=%s time='%s'" % \
            (str(self.sgt_host), str(self.sgt_time))
        print "PP Info:\t host=%s time='%s'" % \
            (str(self.pp_host), str(self.pp_time))
        print "Comment:\t '%s'" % (str(self.comment))
        print "Tracking:\t user='%s' job_id='%s'" % \
            (str(self.last_user), str(self.job_id))
        print "Submit Dir:\t %s" % (str(self.submit_dir))
        print "Notify User:\t %s" % (str(self.notify_user))
        return 0
                                                                            
