#!/usr/bin/env python

# Add RunManager modules to PYTHONPATH
import sys
sys.path.append('/home/scec-00/patrices/code/trunk/RunManager/')


# General imports
import time
import socket
from RunManager import *
from Condor import *


# Globals
class info:
    host = None

# Specifies how to map an execution state to its corresponding error state
ERROR_STATE_MAP = {"SGT Started" : "SGT Error", "PP Started" : "PP Error",}


def init():
    global info

    # Retrieve local host name
    info.host = socket.gethostname()
    info.host = info.host.split(".")[0]

    return 0


def main():
    global info

    print "-- Starting up at %s --" % (time.strftime("%Y-%m-%d %H:%M:%S"))

    rm = RunManager(readonly=False)
    rm.useHTML(False)

    # Get a list of all running jobs from condor
    condor = Condor()
    if (condor.cacheAllJobs() != 0):
        # No work is possible
        print "-- Shutting down at %s --" % (time.strftime("%Y-%m-%d %H:%M:%S"))
        return 0

    searchrun = Run()
    for s,e in ERROR_STATE_MAP.items():
        update_list = []
        searchrun.setStatus(s)
        print "Querying DB for runs in '%s' state" % (s)
        matches = rm.getRuns(searchrun, lock=True)
        if (matches != None):
            print "Found %d run(s)" % (len(matches))
            for run in matches:
                print "Examining run %d (site %s)" % (run.getRunID(), run.getSiteName())
                if ((run.getJobID() == None) or (run.getJobID() == "")):
                    print "Run %d is in an execution state with no JobID assigned" % (run.getRunID())
                    run.setStatus(e)
                    update_list.append(run)
                else:
                    # Check run's job_id in condor
                    (host, job_id,) = run.getJobID().split(":", 1)
                    if (host != info.host):
                        print "Ignoring job submitted on host %s" % (host)
                    else:
                        job = condor.getJobFromCache(job_id)
                        if (job == None):
                            print "No job '%s' found for run %d" % (job_id, run.getRunID())
                            run.setStatus(e)
                            update_list.append(run)
                        else:
                            print "Running job found. OK."

            if (len(update_list) > 0):
                for run in update_list:
                    # Perform update
                    print "Changing state of run %d to %s" % (run.getRunID(), run.getStatus())
                    if (rm.updateRun(run) != 0):
                        # Abort updating this batch by performing a rollback
                        print "Failed to update run %d (site %s). Performing rollback." % \
                            (run.getRunID(), run.getSiteName())
                        rm.rollbackTransaction()
                        break
                    print "Successfully updated run %d (site %s)." % (run.getRunID(), run.getSiteName())      

                # Commmit these updates
                print "Committing changes"
                rm.commitTransaction()

            else:
                print "Releasing records"
                rm.rollbackTransaction()

        else:
            print "No runs found in state '%s'" % (s)

    print "-- Shutting down at %s --" % (time.strftime("%Y-%m-%d %H:%M:%S"))
    return 0


def cleanup():
    return 0
    
    
if __name__ == '__main__':
    if (init() != 0):
        sys.exit(1)
    retval = main()
    cleanup()
    sys.exit(retval)
    
