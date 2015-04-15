#!/usr/bin/env python


# Imports
import os
import sys
import time
import subprocess
from CondorJob import *


# Globals
CONDOR_Q = "condor_q"
CONDOR_RETRY = 3
CONDOR_WAIT_SECS = 30

CONDOR_HOME = "/usr/local/condor/default/bin"

class Condor:
    job_cache = {}

    def __init__(self):
        job_cache = {}
        return

    def __runCommand(self, cmd):
        try:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            output = p.communicate()[0]
            retcode = p.returncode
            if retcode != 0:
                #print output
                #print "Non-zero exit code"
                return None
        except:
            #print sys.exc_info()
            #print "Failed to run cmd: " + str(cmd)
            return None

        output = output.splitlines()
        return output


    def __runCondorCommand(self, cmd):

        # Under high scheduler load, will get failure to connect error. In this case
        # retry the query
        count = 0
        done = False
        while ((count < CONDOR_RETRY) and (not done)):
            output = self.__runCommand(cmd)
            if (output == None):
                print "Failed to execute condor command."
                count = count + 1
                print "Waiting " + str(CONDOR_WAIT_SECS) + " secs, then retrying"
                time.sleep(CONDOR_WAIT_SECS)
            else:
                done = True

        if (not done):
            return None
        else:
            return output


    def __parseClassAds(self, classads):
        # Check to ensure there is a classad
        if (len(classads) <= 4):
            return None

        classad_dict = {}
        for ad in classads:
            tokens = ad.split(" = ", 1)
            if (len(tokens) == 2):
                classad_dict[tokens[0]] = tokens[1]
            #else:
            #    print "Invalid classad (%s)" % (ad)
        if (len(classad_dict.keys()) == 0):
            return None
        else:
            return classad_dict


    def getJobs(self):
        return None


    def getJob(self, job_id):
        # Method returns tuple (job object, error code) in order to allow 
        # apps to distinguish between condor_q errors and 
        # non-existance of a job

        if ((job_id == None) or (str(job_id) == "")):
            return (None, 0)

        # Query condor_q for this job's classad
        condorcmd = ["%s/%s" % (CONDOR_HOME, CONDOR_Q), '-long', str(job_id)]
        output = self.__runCondorCommand(condorcmd)
        if (output == None):
            return (None, 1)
        
        classads = self.__parseClassAds(output)
        if (classads == None):
            return (None, 0)

        # Parse the classad
        job = CondorJob()
        job.setJobID(job_id)
        for k,v in classads.items():
            if (k == 'DAGManJobId'):
                job.setParent(v)
            elif (k == "JobStatus"):
                job.setStatus(int(v))
            elif (k == 'Cmd'):
                job.setCommand(v)

        return (job, 0)


    def cacheAllJobs(self):

        # Query condor_q for this job's classad
        condorcmd = ["%s/%s" % (CONDOR_HOME, CONDOR_Q),]
        output = self.__runCondorCommand(condorcmd)
        if (output == None):
            return 1

        if (len(output) < 6):
            self.job_cache = {}
            return 0
        
        # Strip off beginning 4 lines, last 2 lines
        output = output[4:-2]

        for line in output:
            job_id = line.split()[0].split(".")[0]
            # Add new entry in job cache
            job = CondorJob()
            job.setJobID(job_id)
            self.job_cache[job_id] = job

        return 0


    def getJobFromCache(self, job_id):
        if ((job_id == None) or (str(job_id) == "")):
            return None

        if (str(job_id) in self.job_cache.keys()):
            job = self.job_cache[str(job_id)]
            return job

        return None
