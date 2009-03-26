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
    update_scatter_plot = None
    #plot_list = None

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

    info.update_scatter_plot = False
    #info.plot_list = []

    return 0


def runCommand(cmd):
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        output = p.communicate()[0]
        retcode = p.returncode
        if retcode != 0:
            print output
            print "Non-zero exit code"
            return None
    except:
        print sys.exc_info()
        print "Failed to run cmd: " + str(cmd)
        return None

    output = output.splitlines()
    return output



def sendMessage(run, m, msgstr):
    notify_list = run.getNotifyUserAsList()

    subject = str(MSG_SUBJECT % (run.getRunID(), run.getSite().getShortName()))
    msg = str(MSG_FORMAT % (run.getRunID(), run.getSite().getShortName(), msgstr))

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
                print "Examining run %d (site %s)" % (run.getRunID(), run.getSite().getShortName())
                if ((run.getJobID() == None) or (run.getJobID() == "")):
                    if (not run.getStatus() in ["Initial", "SGT Generated",]):
                        sendMessage(run, m, \
                                        "In the execution state '%s' with no Job ID assigned. Moved to state '%s'." % \
                                        (run.getStatus(), e))
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
                            (run.getRunID(), run.getSite().getShortName())
                        rm.rollbackTransaction()
                        break
                    print "Successfully updated run %d (site %s)." % (run.getRunID(), run.getSite().getShortName())      

                # Commmit these updates
                print "Committing changes"
                rm.commitTransaction()

            else:
                print "Releasing records"
                rm.rollbackTransaction()

        else:
            print "No runs found in state '%s'" % (s)

    return 0


def hasCompCurves(run):
    return True


def checkVerifyErrors(rm, m):
    global info

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
                    # Check for comparison curves
                    if (hasCompCurves(run) == True):
                        new_state = DONE_STATE
                        sendMessage(run, m, "Run passed verification checks. Moved to state '%s'." % (new_state))
                        run.setComment("Passed verification checks")
                    else:
                        new_state = PLOT_STATE
                        sendMessage(run, m, "Site needs comparison curves. Moved to state '%s'." % (new_state))
                        run.setComment("Generating comparison curves")
                    run.setStatus(new_state)
                    update_list.append(run)
                    info.update_scatter_plot = True
                    #info.plot_list.append(run)
                else:
                    sendMessage(run, m, "Run failed verification checks (num_psa == 0 or num_curves == 0). Moved to state '%s'." % (CHECK_ERROR_STATE))
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
                        (run.getRunID(), run.getSite().getShortName())
                    rm.rollbackTransaction()
                    break
                print "Successfully updated run %d (site %s)." % (run.getRunID(), run.getSite().getShortName())      

            # Commmit these updates
            print "Committing changes"
            rm.commitTransaction()

        else:
            print "Releasing records"
            rm.rollbackTransaction()

    else:
        print "No runs found in state '%s'" % (CHECK_VERIFY_STATE)

    return 0


def createScatterPlot():

    print "Generating new scatter plot"

    plot_cmd = ['ssh', OPENSHA_LOGIN, OPENSHA_SCATTER_SCRIPT,]
    retval = runCommand(plot_cmd)
    if (retval == None):
        print "Failed to create new scatter plot"
    else:
        print "Successfully created new scatter plot"

    return 0


def createCompCurves(rm):

    print "Generating comparison curves."

    # Query for list of runs in plotting state
    run_list = rm.getRunsByState([PLOT_STATE], lock=True)
    if ((run_list == None) or (len(run_list) == 0)):
        print "No runs in state '%s'. No work to do." % (PLOT_STATE)
        return 0

    for run in run_list:
        print "Generating comparison curves for run %d (site %s)" % \
            (run.getRunID(), run.getSite().getShortName())

        # NOTE: For each run, should query DB for curves and generate for each PSA value
        # rather than use hard-coded list

        # Construct paths
        output_dir = '%s%s' % (CURVE_DIR, run.getSite().getShortName())

        # Generate comparison curves
        plot_cmd = [OPENSHA_CURVE_SCRIPT, \
                        '-R', '%d' % (run.getRunID()), \
                        '-s', '%s' % (run.getSite().getShortName()), \
                        '-n', \
                        '-o', output_dir, \
                        '-ef', OPENSHA_ERF_XML, \
                        '-af', OPENSHA_AF_XML, \
                        '-t', 'png,pdf', \
                        '-pf', OPENSHA_DBPASS_FILE, \
                        '-p', '3,5,10',]
        retval = runCommand(plot_cmd)
        if (retval == None):
            print "Failed to create new comparison curves"
            run.setState(CHECK_ERROR_STATE)
            run.setComment("Failed verification checks")
            sendMessage(run, m, "Run failed verification checks (num_psa == 0 or num_curves == 0). Moved to state '%s'." % (CHECK_ERROR_STATE))
        else:
            print "Successfully created new comparison curves"
            run.setState(DONE_STATE)
            run.setComment("Passed verification checks")
            sendMessage(run, m, "Run passed verification checks. Moved to state '%s'." % (DONE_STATE))

        # Perform db update
        if (rm.updateRun(run) != 0):
            print "Failed to update run %d (site %s)." % \
                (run.getRunID(), run.getSite().getShortName())
            # Keep going and try to update as many runs as possible
        
        # Commmit these updates
        rm.commitTransaction()
        
    return 0


def main():

    print "-- Starting up at %s --" % (time.strftime("%Y-%m-%d %H:%M:%S"))

    rm = RunManager(readonly=False)
    rm.useHTML(False)

    m = Mailer()

    checkExecErrors(rm, m)
    checkVerifyErrors(rm, m)

    # Generate a new scatter plot if needed
    if (info.update_scatter_plot == True):
        if (createScatterPlot() != 0):
            print "Warning: Failed to produce scatter plot."

    # Generate comparison curves if needed
    createCompCurves(rm)

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
    
