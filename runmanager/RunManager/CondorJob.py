#!/usr/bin/env python


# Imports
import sys
import time


# Globals
CONDOR_DAGMAN = "condor_dagman"


class CondorJob:
    job_id = None
    status = None
    parent = None
    cmd = None
    status_time = None
    num_starts = None


    def __init__(self):
        self.job_id = None
        self.status = None
        self.parent = None
        self.cmd = None
        self.status_time = None
        self.num_starts = None


    def getJobID(self):
        return self.job_id


    def setJobID(self, job_id):
        if (job_id == None):
            self.job_id = None
        else:
            self.job_id = str(job_id)


    def getStatus(self):
        return self.status


    def setStatus(self, status):
        if (status == None):
            self.status = None
        else:
            self.status = int(status)


    def isDAG(self):
        if ((self.cmd != None) and (self.cmd.find(CONDOR_DAGMAN) != -1)):
            return True
        else:
            return False


    def isChild(self):
        if (self.parent != None):
            return True
        else:
            return False


    def getCommand(self):
        return self.command


    def setCommand(self, cmd):
        if (cmd == None):
            self.cmd = None
        else:
            self.cmd = str(cmd)


    def getParent(self):
        return self.parent


    def setParent(self, parent):
        if (parent == None):
            self.parent = None
        else:
            self.parent = int(parent)


    def getStatusTime(self):
        return self.status_time


    def setStatusTime(self, status_time):
        if (status_time == None):
            self.status_time = None
        else:
            self.status_time = int(status_time)


    def getNumStarts(self):
        return self.num_starts


    def setNumStarts(self, num_starts):
        if (num_starts == None):
            self.num_starts = None
        else:
            self.num_starts = int(num_starts)
