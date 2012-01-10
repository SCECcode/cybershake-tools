#!/usr/bin/env python


# Add RunManager modules to PYTHONPATH
import sys
sys.path.append('/home/scec-00/patrices/code/trunk/RunManager/')


# General imports
from Config import *
from RunManager import *

# Constants
PLAN_STAGES = {"SGT_PLAN":["Initial", "SGT Error",], "PP_PLAN":["SGT Generated", "PP Error"]}


# Globals
class info:
    pass



def init():
    global info
    
    # Get number of command-line arguments
    argc = len(sys.argv)
    
    # Check command line arguments
    if (argc < 3):
        print "Usage: " + sys.argv[0] + " <run_id> <site> <plan stage>"
        print "Example: " + sys.argv[0] + " 213 SGT_PLAN"
        print "Valid Plan Stages: " + str(PLAN_STAGES.keys())
        return 1

    # Parse command line args and options
    info.run_id = int(sys.argv[1])
    info.site = sys.argv[2]
    info.stage = sys.argv[3]

    print "Configuration:"
    print "Run ID:\t" + str(info.run_id)
    print "Site:\t" + str(info.site)
    print "Stage:\t" + str(info.stage)  
    
    return 0


def main():
    global info

    rm = RunManager()
    rm.useHTML(False)

    # Retrieve existing run info and save it
    run = rm.getRunByID(info.run_id, lock=False)
    if (run == None):
        print "No record for run_id %d found in DB." % (info.run_id)
        return 1

    if (run.getSite().getShortName() != info.site):
            print "Found site %s in run %d, expected site %s" % \
                (run.getSite().getShortName(), run.getRunID(), info.site)
            return 1
        
    if (info.stage == "SGT_PLAN"):
        if (run.getStatus() in PLAN_STAGES["SGT_PLAN"]):
            print "Run %d is valid" % (run.getRunID())
            return 0
        else:
            print "Run %d is invalid" % (run.getRunID())
            return 1
    elif (info.stage == "PP_PLAN"):
        if (run.getStatus() in PLAN_STAGES["PP_PLAN"]):
            print "Run %d is valid" % (run.getRunID())
            return 0
        else:
            print "Run %d is invalid" % (run.getRunID())
            return 1

    return 0


def cleanup():
    return 0
    
    
if __name__ == '__main__':
    if (init() != 0):
        sys.exit(1)
    retval = main()
    cleanup()
    sys.exit(retval)
    
