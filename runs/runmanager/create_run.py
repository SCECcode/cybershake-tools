#!/usr/bin/env python

# Add RunManager modules to PYTHONPATH
import sys
sys.path.append('/home/scec-00/patrices/code/trunk/RunManager/')


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
    if ((argc != 2) and (argc != 5)):
        print "Usage: " + sys.argv[0] + " <site> [erf id] [sgt var id] [rup var id]"
        print "Example: " + sys.argv[0] + " USC 34 5 3"
        return 1

    # Parse command line args
    info.site = sys.argv[1]
    if (argc == 5):
        info.erf = int(sys.argv[2])
        info.sgt_var = int(sys.argv[3])
        info.rup_var = int(sys.argv[4])
    else:
        info.erf = None
        info.sgt_var = None
        info.rup_var = None
    
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
        print "Failed to instantiate run manager."
        return 1

    rm.beginTransaction()

    if (info.erf == None):
        run = rm.createRunBySite(info.site)
    else:
        run = rm.createRunByParam(info.site, info.erf, info.sgt_var, info.rup_var)

    if (run == None):
        print "Run insert failed."
        rm.rollbackTransaction()
        return 1

    # Commit the changes
    rm.commitTransaction()
    
    #run.dumpToScreen()
    print run.getRunID()

    return 0


def cleanup():
    return 0
    
    
if __name__ == '__main__':
    if (init() != 0):
        sys.exit(1)
    retval = main()
    cleanup()
    sys.exit(retval)

