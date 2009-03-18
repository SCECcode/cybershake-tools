#!/usr/bin/env python


# General imports
from Config import *
from RunManager import *


# Constants

# Map of acceptable run states for each reported workflow state
RUN_STATES = {"SGT_INIT":["Initial"], \
                  "SGT_START":["SGT Started"], \
                  "SGT_END":[], \
                  "PP_INIT":["SGT Generated"], \
                  "PP_START":["PP Started"], \
                  "PP_END":[]}

# Map of workflow state to run state. Assumes no errors were encountered.
STATE_MAP = {"SGT_INIT":"Initial", \
                 "SGT_START":"SGT Started", \
                 "SGT_END":"SGT Generated", \
                 "PP_INIT":"SGT Generated", \
                 "PP_START":"PP Started", \
                 "PP_END":"Curves Generated"}

# Globals
class info:
    pass


def init():
    global info
    
    # Get number of command-line arguments
    argc = len(sys.argv)
    
    # Check command line arguments
    if (argc < 4):
        print "Usage: " + sys.argv[0] + " <run_id> <old_state> <new_state>"
        print "Example: " + sys.argv[0] + " 213 SGT_START SGT_END"
        print "Valid Run States: " + str(RUN_STATES.keys())
        return 1

    # Parse command line args and options
    info.run_id = int(sys.argv[1])
    info.old_state = sys.argv[2]
    info.new_state = sys.argv[3]
    
    print "Configuration:"
    print "Run ID:\t\t" + str(info.run_id)
    print "Old State:\t" + info.old_state
    print "New State:\t" + info.new_state + "\n"

    if (info.old_state == info.new_state):
        print "No state change specified."
        return 1

    if (not info.old_state in RUN_STATES.keys()):
        print "Old state '%s', expecting one of '%s'" % \
            (info.old_state, str(RUN_STATES.keys()))
        return 1

    if (not info.new_state in RUN_STATES.keys()):
        print "New state '%s', expecting one of '%s'" % \
            (info.old_state, str(RUN_STATES.keys()))
        return 1

    if (not STATE_MAP[info.new_state] in STATUS_STD[STATE_MAP[info.old_state]]):
        print "Invalid state change requested, from '%s' to '%s'" % \
            (info.old_state, info.new_state)
        return 1

    return 0



def main():
    global info

    rm = RunManager(readonly=False)
    rm.useHTML(False)

    old_run_states = RUN_STATES[info.old_state]
    new_run_state = STATE_MAP[info.new_state]
    
    # Retrieve existing run info and save it
    run = rm.getRunByID(info.run_id, lock=True)
    if (run == None):
        print "No record for run_id %d found in DB." % (info.run_id)
        return 1

    # Check that we're in the expected state
    if (not run.getStatus() in old_run_states):
        rm.rollbackTransaction()
        print "Run has state '%s', was expecting to find one of %s" % \
            (run.getStatus(), str(old_run_states))
        return 1
    
    saverun = Run()
    saverun.copy(run)

    # Update the state
    run.setStatus(new_run_state)
    run.setStatusTimeCurrent()

    # Done by update method
    #if (new_run_state in SGT_STATES):
    #    run.setSGTTimeCurrent()
    #if (new_run_state in PP_STATES):
    #    run.setPPTimeCurrent()

    if ((info.new_state == "SGT_END") or (info.new_state == "PP_END")):
        # Clear the Job_ID at end of workflow
        run.setJobID("")

    print "Updated Record:"
    run.dumpToScreen()

    # Perform update
    if (rm.updateRun(run, orig_run=saverun) != 0):
        rm.rollbackTransaction()
        print "Run update failed for run_id %d (site %s)." % \
            (run.getRunID(), run.getSiteName())
        return 1
    else:
        rm.commitTransaction()
        print ""
        print "Run successfully updated for run_id %d (site %s)." % \
            (run.getRunID(), run.getSiteName())

    return 0


def cleanup():
    return 0
    
    
if __name__ == '__main__':
    if (init() != 0):
        sys.exit(1)
    retval = main()
    cleanup()
    sys.exit(retval)
    
