#!/usr/bin/env python

# Add RunManager modules to PYTHONPATH
import sys
sys.path.append('/home/scec-00/patrices/code/trunk/RunManager/')


# General imports
import time
import socket
from RunManager import *
from Condor import *
from Mailer import *


# Globals
class info:
    host = None

# Format for out-going email msgs
MSG_SUBJECT = "Run %d (site %s) experienced an error"
MSG_FORMAT = "Run %d (site %s) encountered the following problem:\r\n\r\n%s\r\n"

# Specifies how to map a run state to its corresponding error state for those runs
# with a job_id assigned that cannot be found in condor_q
ERROR_STATE_MAP = {"SGT Started" : "SGT Error", "Initial":"SGT Error", \
                       "PP Started" : "PP Error", "SGT Generated":"PP Error"}

# Runs in this state are ready for verification
CHECK_VERIFY_STATE = "Curves Generated"
CHECK_ERROR_STATE = "Verify Error"


def init():
    global info

    # Retrieve local host name
    info.host = socket.gethostname()
    info.host = info.host.split(".")[0]

    return 0


def sendMessage(run, m, msgstr):
    notify_list = run.getNotifyUserAsList()

    subject = str(MSG_SUBJECT % (run.getRunID(), run.getSiteName()))
    msg = str(MSG_FORMAT % (run.getRunID(), run.getSiteName(), msgstr))

    if (notify_list != None):
        m.send(notify_list, subject, msg)

    return 0


def checkExecErrors(rm, m):
    global info

    # Get a list of all running jobs from condor
    condor = Condor()
    if (condor.cacheAllJobs() != 0):
        # No work is possible
        print "Failed to connect to condor."
        return 1

    # Check for runs in an error state
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
                    if (not run.getStatus() in ["Initial", "SGT Generated",]):
                        sendMessage(run, m, "In the execution state '%s' with no Job ID assigned. Moved to state '%s'." % (run.getStatus(), e))
                        print "Run %d is in an execution state with no JobID assigned" % (run.getRunID())
                        run.setStatus(e)
                        run.setComment("Inconsistent state detected")
                        update_list.append(run)
                else:
                    # Check run's job_id in condor
                    (host, job_id,) = run.getJobID().split(":", 1)
                    if (host != info.host):
                        print "Ignoring job submitted on host %s" % (host)
                    else:
                        job = condor.getJobFromCache(job_id)
                        if (job == None):
                            sendMessage(run, m, "No job '%s' found in condor. Moved to state '%s'." % (job_id, e))
                            print "No running condor job '%s' found for run %d" % (job_id, run.getRunID())
                            run.setStatus(e)
                            run.setComment("Workflow terminated unexpectedly")
                            update_list.append(run)
                        else:
                            print "Running job found. OK."

            if (len(update_list) > 0):
                for run in update_list:
                    # Perform update
                    print "Changing state of run %d to '%s'" % (run.getRunID(), run.getStatus())
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

    return 0


def checkVerifyErrors(rm, m):
    # Check for ready-to-verify runs
    searchrun = Run()
    searchrun.setStatus(CHECK_VERIFY_STATE)
    print "Querying DB for runs in '%s' state to verify" % (CHECK_VERIFY_STATE)
    update_list = []
    matches = rm.getRuns(searchrun, lock=True)
    if (matches != None):
        for run in matches:
            stats = rm.getRunStatsByID(run.getRunID())
            if (stats == None):
                print "Failed to collect stats for run %d" % (run.getRunID())
            else:
                if ((stats.getNumPSAs() > 0) and (stats.getNumCurves() > 0)):
                    sendMessage(run, m, "Database passed verification checks. Moved to state '%s'." % (DONE_STATE))
                    run.setStatus(DONE_STATE)
                    run.setComment("Passed verification checks")
                    update_list.append(run)
                else:
                    sendMessage(run, m, "Database failed verification checks (num_psa == 0 or num_curves == 0). Moved to state '%s'." % (CHECK_ERROR_STATE))
                    print "Run %d failed verification" % (run.getRunID())
                    run.setStatus(CHECK_ERROR_STATE)
                    run.setComment("Failed verification checks")
                    update_list.append(run)

        if (len(update_list) > 0):
            for run in update_list:
                # Perform update
                print "Changing state of run %d to '%s'" % (run.getRunID(), run.getStatus())
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
        print "No runs found in state '%s'" % (CHECK_VERIFY_STATE)

    return 0


def main():

    print "-- Starting up at %s --" % (time.strftime("%Y-%m-%d %H:%M:%S"))

    rm = RunManager(readonly=False)
    rm.useHTML(False)

    m = Mailer()

    checkExecErrors(rm, m)
    checkVerifyErrors(rm, m)

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
    
