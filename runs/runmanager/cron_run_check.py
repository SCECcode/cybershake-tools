#!/usr/bin/env python

# Add RunManager modules to PYTHONPATH
import sys
sys.path.append('/home/scec-02/cybershk/runs/runmanager/RunManager/')


# General imports
import time
import socket
from RunManager import *
from Condor import *
from Mailer import *
from Config import *

# Globals
class info:
    host = None
    update_maps = None
    master = None

# Format for out-going email msgs
MSG_SUBJECT = "Run %d (site %s) status update"
MSG_FORMAT = "Run %d (site %s) encountered the following issue:\r\n\r\n%s\r\n"

# Specifies how to map a run state to its corresponding error state for those runs
# with a job_id assigned that cannot be found in condor_q
ERROR_STATE_MAP = {"SGT Started" : "SGT Error", "Initial":"SGT Error", \
                       "PP Started" : "PP Error", "SGT Generated":"PP Error"}

# Runs in this state are ready for verification
CHECK_VERIFY_STATE = "Curves Generated"
CHECK_ERROR_STATE = "Verify Error"


def init():
    global info

    # Get number of command-line arguments
    argc = len(sys.argv)
    
    # Check command line arguments
    if (argc < 2):
        print "Usage: " + sys.argv[0] + " <mode>"
        print "Example: " + sys.argv[0] + " MASTER/SECONDARY"
        return 1

    # Parse command line args and options
    if (sys.argv[1] == 'MASTER'):
        info.master = True
    else:
        info.master = False

    # Retrieve local host name
    info.host = socket.gethostname()
    info.host = info.host.split(".")[0]

    info.update_maps = False

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
    for s,e in ERROR_STATE_MAP.items():
        update_list = []
        searchrun = Run()
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
                            sendMessage(run, m, "No job '%s' found in condor. Moved to state '%s'." % (run.getJobID(), e))
                            print "No running condor job '%s' found for run %d" % (run.getJobID(), run.getRunID())
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
                        print "Failed to update run %d (site %s). Performing rollback." % \
                            (run.getRunID(), run.getSite().getShortName())
                        # Keep trying to update
                    else:
                        print "Successfully updated run %d (site %s)." % (run.getRunID(), run.getSite().getShortName())      
            # Commmit the successful updates
            #print "Committing changes"
            #rm.commitTransaction()

        else:
            print "No runs found in state '%s'" % (s)

        # Commmit the successful updates
        print "Committing changes"
        rm.commitTransaction()

    return 0


def hasCompCurves(run, stats):

    # Construct path
    output_dir = '%s%s' % (CURVE_DIR, run.getSite().getShortName())

    # Get list of curves from src dir
    try:
        files = os.listdir(src_dir)
    except:
        files = []

    # Isolate the .png files
    curves = {}
    if (len(files) > 0):
        # Display all .png files
        i = 0
        for f in files:
            if (f.find('.png') != -1):
                i = i + 1
                srcname = "%s%s" % (src_dir, f)
                escaped = cgi.escape(srcname, True)
                curves[srcname] = escaped

    # Identify missing comparison curves
    for c in stats.getCurveList():
        per_str = "%dsec" % (int(round(c.getIMValue())))
        found = False
        for fname,escname in curves.items():
            if (fname.find("SA_%s" % (per_str)) != -1):
                found = True
                break;
        if (not found):
            return False

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
                    if (hasCompCurves(run, stats) == True):
                        new_state = DONE_STATE
                        sendMessage(run, m, "Run passed verification checks. Moved to state '%s'." % (new_state))
                        run.setComment("Passed verification checks")
                    else:
                        new_state = PLOT_STATE
                        sendMessage(run, m, "Site needs comparison curves. Moved to state '%s'." % (new_state))
                        run.setComment("Generating comparison curves")
                    run.setStatus(new_state)
                    update_list.append(run)
                    info.update_maps = True
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
                    print "Failed to update run %d (site %s)" % \
                        (run.getRunID(), run.getSite().getShortName())
                    # Keep trying to update
                else:
                    print "Successfully updated run %d (site %s)." % (run.getRunID(), run.getSite().getShortName())

        # Commmit the successful updates
        print "Committing changes"
        rm.commitTransaction()

    else:
        print "No runs found in state '%s'" % (CHECK_VERIFY_STATE)

    return 0


def createMaps():

    print "Generating new scatter map"
    plot_cmd = ['ssh', OPENSHA_LOGIN, OPENSHA_SCATTER_SCRIPT,]
    retval = runCommand(plot_cmd)
    if (retval == None):
        print "Failed to create new scatter map"
    else:
        print "Successfully created new scatter map"

    print "Generating new interpolated map for all sites"
    plot_cmd = ['ssh', OPENSHA_LOGIN, OPENSHA_INTERPOLATED_SCRIPT,]
    retval = runCommand(plot_cmd)
    if (retval == None):
        print "Failed to create new interpolated map for all sites"
    else:
        print "Successfully created new interpolated map for all sites"

    print "Generating new interpolated map for gridded sites"
    plot_cmd = ['ssh', OPENSHA_LOGIN, OPENSHA_INTERPOLATED_SCRIPT, '4',]
    retval = runCommand(plot_cmd)
    if (retval == None):
        print "Failed to create new interpolated map for gridded sites"
    else:
        print "Successfully created new interpolated map for gridded sites"

    return 0


def createCompCurves(rm, m):

    # Save current working dir and move to opensha
    cwd = os.getcwd()
    print "Changing dir to %s" % (OPENSHA_DIR)
    os.chdir(OPENSHA_DIR)

    print "Querying for runs needing comparison curves."

    # Query for list of runs in plotting state
    run_list = rm.getRunsByState([PLOT_STATE], lock=True)
    if ((run_list == None) or (len(run_list) == 0)):
        print "No runs in state '%s'. No work to do." % (PLOT_STATE)
        # Perform release just in case
        rm.rollbackTransaction()
        return 0

    print "Found %d runs needing curves." % (len(run_list))

    for run in run_list:

        stats = rm.getRunStatsByID(run.getRunID())
        if (stats == None):
            print "Failed to collect stats for run %d" % (run.getRunID())
        else:
            print "Generating comparison curves for run %d (site %s)" % \
                (run.getRunID(), run.getSite().getShortName())

            # Construct list of periods to compute comparison curves
            period_list = ''
            for c in stats.getCurveList():
                if (period_list == ''):
                    period_list = str(int(round(c.getIMValue())))
                else:
                    period_list = '%s,%s' % (period_list, str(int(round(c.getIMValue()))))

            # Construct path
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
                            '-p', period_list,]
            print str(plot_cmd)
            retval = runCommand(plot_cmd)
            if (retval == None):
                print "Failed to create new comparison curves"
                run.setStatus(CHECK_ERROR_STATE)
                run.setComment("Failed to create comparison curves")
                sendMessage(run, m, "Failed to generate comparison curves. Moved to state '%s'." % (CHECK_ERROR_STATE))
            else:
                print "Successfully created new comparison curves"
                run.setStatus(DONE_STATE)
                run.setComment("Passed verification checks")
                sendMessage(run, m, "Run passed verification checks. Moved to state '%s'." % (DONE_STATE))

            # Perform db update
            if (rm.updateRun(run) != 0):
                print "Failed to update run %d (site %s)." % \
                    (run.getRunID(), run.getSite().getShortName())
                # Keep going and try to update as many runs as possible
            else:
                print "Successfully updated run %d (site %s)." % (run.getRunID(), run.getSite().getShortName())

    # Commmit the successful updates
    rm.commitTransaction()

    # Return to original dir
    os.chdir(cwd)

    return 0


def main():

    print "-- Starting up at %s --" % (time.strftime("%Y-%m-%d %H:%M:%S"))

    rm = RunManager(readonly=False)
    rm.useHTML(False)

    m = Mailer()

    # Look for workflow execution errors
    checkExecErrors(rm, m)

    # Checks to perform only on the master RunManager host
    if (info.master == True):
        # Perform verification checks on completed runs
        checkVerifyErrors(rm, m)

        # Generate new maps if needed
        if (info.update_maps == True):
            if (createMaps() != 0):
                print "Warning: Failed to produce maps."

        # Generate comparison curves if needed
        createCompCurves(rm, m)


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
    
