#!/usr/bin/env python


# Add RunManager modules to PYTHONPATH
import sys
sys.path.append('/home/scec-00/patrices/code/trunk/RunManager/')


# General imports
from Config import *
from RunManager import *

# Constants
VALID_FIELDS = ["Status", "SGT_Host", "PP_Host", "Comment", "Last_User", "Job_ID", "Submit_Dir",]


# Globals
class info:
    pass

updates = {}
override = False


def init():
    global info
    global updates
    global override
    
    # Get number of command-line arguments
    argc = len(sys.argv)
    
    # Check command line arguments
    if (argc < 3):
        print "Usage: " + sys.argv[0] + " <run_id> [-o override constraints] <field1=value1> [field2=value2] ... [fieldn=valuen]"
        print "Example: " + sys.argv[0] + " 213 state=\"Deleted\""
        print "Valid Field Tags: " + str(VALID_FIELDS)
        return 1

    # Parse command line args and options
    info.run_id = int(sys.argv[1])
    for token in sys.argv[2:]:
        if (token[0] == '-'):
            option = token[1:]
            if (option == "o"):
                override = True
        else:
            tokens = token.split("=", 1)
            if (tokens[0] in VALID_FIELDS):
                updates[tokens[0]] = tokens[1]
            else:
                print "Unrecognized token %s" % (str(token))
                return 1
                
    print "Configuration:"
    print "Run ID:\t\t" + str(info.run_id)
    print "Edits:\t"
    for f,v in updates.items():
        print "\t\t%s = %s" % (f, v)
    if (override):
        print "Override:\tIgnoring state-transition constraints"
    else:
        print "Override:\tState-transition constraints enforced"
    print "\n"

    return 0


def updateFields(oldrun):

    # Unless overridden later, set last_user to be the current user_id
    oldrun.setLastUserCurrent()

    oldrun.setStatusTimeCurrent()
    for k,v in updates.items():
        if (k == "Status"):
            # Check that state is valid
            if (not override):
                if (not v in STATUS_STD[oldrun.getStatus()]):
                    print "Invalid state %s provided, expecting one of %s." % (v, str(STATUS_STD[oldrun.getStatus()]))
                    return None
            else:
                if (not v in STATUS_STD.keys()):
                    print "Invalid state %s provided, expecting one of %s." % (v, str(STATUS_STD.keys()))
                    return None
            oldrun.setStatus(v)
            if (v in SGT_STATES):
                oldrun.setSGTTimeCurrent()
            elif (v in PP_STATES):
                oldrun.setPPTimeCurrent()
        elif (k == "SGT_Host"):
            if (not v in HOST_LIST):
                print "Invalid host %s provided, expecting one of %s." % (v, str(HOST_LIST))
                return None
            oldrun.setSGTHost(v)
            oldrun.setSGTTimeCurrent()
        elif (k == "PP_Host"):
            if (not v in HOST_LIST):
                print "Invalid host %s provided, expecting one of %s." % (v, str(HOST_LIST))
                return None
            oldrun.setPPHost(v)
            oldrun.setPPTimeCurrent()
        elif (k == "Comment"):
            oldrun.comment = v
        elif (k == "Last_User"):
            if (not v in USER_LIST):
                print "Invalid user %s provided, expecting one of %s." % (v, str(USER_LIST))
                return None
            oldrun.setLastUser(v)
        elif (k == "Job_ID"):
            oldrun.setJobID(v)
        elif (k == "Submit_Dir"):
            oldrun.setSubmitDir(v)

    return oldrun


def main():
    global info

    rm = RunManager(readonly=False)
    rm.useHTML(False)

    # Retrieve existing run info and save it
    run = rm.getRunByID(info.run_id, lock=True)
    if (run == None):
        print "No record for run_id %d found in DB." % (info.run_id)
        return 1

    # NOTE: Should ensure run is not in DELETED_STATE
    
    saverun = Run()
    saverun.copy(run)
    
    print "Original Record:"
    run.dumpToScreen()
    print ""

    # Update the state
    run = updateFields(run)
    if (run == None):
        print "Update failed."
        return 1
    
    print "Updated Record:"
    run.dumpToScreen()

    # Perform update
    if (override):
        retval = rm.updateRun(run)
    else:
        retval = rm.updateRun(run, orig_run=saverun)
    if (retval != 0):
        rm.rollbackTransaction()
        print "Run update failed for run_id %d (site %s)." % (run.getRunID(), run.getSiteName())
        return 1
    else:
        rm.commitTransaction()
        print ""
        print "Run successfully updated for run_id %d (site %s)." % (run.getRunID(), run.getSiteName())


    return 0


def cleanup():
    return 0
    
    
if __name__ == '__main__':
    if (init() != 0):
        sys.exit(1)
    retval = main()
    cleanup()
    sys.exit(retval)
    
