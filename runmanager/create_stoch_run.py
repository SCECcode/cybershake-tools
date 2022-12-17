#!/usr/bin/env python3

# Add RunManager modules to PYTHONPATH
import sys
import os
sys.path.append("%s/RunManager" % os.path.dirname(os.path.realpath(__file__)))


# General imports
from RunManager import *


# Globals
class info:
    pass
    

def init():
    global info
    
    # Get number of command-line arguments
    argc = len(sys.argv)
    
    # Check command line arguments
    if (argc != 7):
        print("Usage: " + sys.argv[0] + " <site> [lf run id] [erf id] [rup var id] [merge freq] [stoch freq]")
        return -1

    # Parse command line args
    info.site = sys.argv[1]
    info.lf_run_id = int(sys.argv[2])
    info.erf = int(sys.argv[3])
    info.rup_var = int(sys.argv[4])
    info.merge_freq = float(sys.argv[5])
    info.stoch_freq = float(sys.argv[6])
    
    #print "Configuration:"
    #print "Site:\t\t" + info.site
    #print "ERF:\t\t" + str(info.erf)
    #print "SGT Var:\t" + str(info.sgt_var)
    #print "Rup Var:\t" + str(info.rup_var) + "\n"

    return 0


def main():
    global info

    rm = RunManager(readonly=False)
    rm.useHTML(False)
    if (not rm.isValid()):
        print("Failed to instantiate run manager.")
        return -1

    lf_run = rm.getRunByID(info.lf_run_id)
    #See if this run is in an OK state
    if lf_run.getStatus()!="Verified":
        print("Low-frequency run ID %d isn't in verified status, so it can't be used, aborting." % info.lf_run_id)
        return -1
 
    #Check that parameters match
    if lf_run.getERFID()!=info.erf or lf_run.getRupVarID()!=info.rup_var or lf_run.getSite().getShortName()!=info.site:
        print("Low-frequency parameters don't match stochastic parameters, aborting.")
        return -2

    rm.beginTransaction()

    if (info.erf == None):
        run = rm.createRunBySite(info.site)
    else:
        run = rm.createRunByParam(info.site, info.erf, lf_run.getSGTVarID(), lf_run.getVelID(), info.rup_var, freq=info.merge_freq, src_freq=lf_run.getSrcFreq(), max_freq=info.stoch_freq, status="SGT Generated", sgthost=lf_run.getSGTHost())

    if (run == None):
        print("Run insert failed.")
        rm.rollbackTransaction()
        return -1

    # Commit the changes
    rm.commitTransaction()
    
    #run.dumpToScreen()
    print(run.getRunID())

    return 0


def cleanup():
    return 0
    
    
if __name__ == '__main__':
    if (init() != 0):
        sys.exit(1)
    retval = main()
    cleanup()
    sys.exit(retval)

