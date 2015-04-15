#!/usr/bin/env python

# Add RunManager modules to PYTHONPATH
import sys
import time
import subprocess
sys.path.append('/home/scec-02/cybershk/runs/runmanager/RunManager/')


# General imports
from RunManager import *
from RC import *

# Constants


# Globals
class info:
    pass

# Constants

# Search for matching runs in one of these states, in descending order of desirability:
#
# 1) most recent run in SGT Generated state
# 2) clone of most recent run in PP Started/PP Error/Curves Generated
MATCH_STATE_DICT = {"USE":["SGT Generated",], \
                        "CLONE":["PP Started", "PP Error", "Curves Generated", "Plotting", "Verified",]}


def init():
    global info
    
    # Get number of command-line arguments
    argc = len(sys.argv)
    
    # Check command line arguments
    if ((argc != 2) and (argc != 8)):
        print "Usage: " + sys.argv[0] + " <site> [erf id] [sgt var id] [rup var id] [vel id] [frequency] [source frequency]"
        print "Example: " + sys.argv[0] + " USC 35 5 3 1 0.5 0.5"
        return 1

    # Parse command line args
    info.site = sys.argv[1]
    if (argc == 8):
        info.erf = int(sys.argv[2])
        info.sgt_var = int(sys.argv[3])
        info.rup_var = int(sys.argv[4])
	info.vel_id = int(sys.argv[5])
	info.awp = False
	if (info.sgt_var==6 or info.sgt_var==8):
		info.awp = True
	info.frequency = float(sys.argv[6])
	info.source_frequency = float(sys.argv[7])
    else:
        info.erf = None
        info.sgt_var = None
        info.rup_var = None
	info.vel_id = None
	info.awp = False
	info.frequency = 0
	info.source_frequency = 0
    
    #print "Configuration:"
    #print "Site:\t\t" + info.site
    #print "ERF:\t\t" + str(info.erf)
    #print "SGT Var:\t" + str(info.sgt_var)
    #print "Rup Var:\t" + str(info.rup_var) + "\n"

    return 0


def cloneLFNs(match, clone, awp):
    
    lfns = ["%s_fx_%d.sgt", \
                "%s_fy_%d.sgt", \
                "%s_fx_%d.sgt.md5", \
                "%s_fy_%d.sgt.md5", ]

    if (awp==True):
	#add sgtheaders
	lfns.append("%s_fx_%d.sgthead")
	lfns.append("%s_fy_%d.sgthead")

    rc = RC()

    for lfn in lfns:
        old_lfn = lfn % (match.getSite().getShortName(), match.getRunID())
        new_lfn = lfn % (clone.getSite().getShortName(), clone.getRunID())

        # Get PFN associated with this LFN
        entry_list = rc.getEntries(old_lfn)
        if ((entry_list == None) or (len(entry_list) == 0)):
            print "Old LFN %s not found." % (old_lfn)
            return 1

        # Take the first entry in the list
        pfn = entry_list[0].pfn
	pool = entry_list[0].pool
	#OK if no pool attribute

        #print "pfn=%s, pool=%s" % (pfn, str(pool))

        # Create new LFN for cloned run
        if (rc.createLFN(new_lfn, pfn, pool=pool) != 0):
            print "Failed to create new LFN %s." % (new_lfn)
            return 1

    return 0


def findMatch(rm, run, match_states):

    pref_match = None

    for s in match_states:
        run.setStatus(s)
        matches = rm.getRuns(run, lock=False)
	if (matches != None):
            for match in matches:
                sgt_time = int(time.mktime(time.strptime(match.getSGTTime(), '%Y-%m-%d %H:%M:%S')))
                if (pref_match == None):
                    pref_match = match
                else:
                    pref_time = int(time.mktime(time.strptime(pref_match.getSGTTime(), '%Y-%m-%d %H:%M:%S')))
                    if (sgt_time > pref_time):
                        pref_match = match
 
    return pref_match


def main():
    global info

    rm = RunManager(readonly=False)
    rm.useHTML(False)

    searchrun = Run()
    searchsite = Site()
    searchsite.setShortName(info.site)
    searchrun.setSite(searchsite)
    if (info.erf != None):
        searchrun.setERFID(info.erf)
        searchrun.setSGTVarID(info.sgt_var)
        searchrun.setRupVarID(info.rup_var)
	searchrun.setVelID(info.vel_id)
	searchrun.setMaxFreq(info.frequency)
	searchrun.setLowFreqCutoff(info.frequency)

    # Find preferred runids, if any
    need_clone = False
    pref_match = findMatch(rm, searchrun, MATCH_STATE_DICT["USE"])
    if (pref_match == None):
        need_clone = True
	pref_match = findMatch(rm, searchrun, MATCH_STATE_DICT["CLONE"])

    if (pref_match == None):
        print "No matching runs found."
        return 1
    
    if (need_clone == True):
        # Clone the run and return new run_id
        clone = Run()
        clone.copy(pref_match)
        clone.setStatus("SGT Generated")
        clone.setStatusTimeCurrent()
        clone.setPPHost(HOST_LIST[0])
        clone.setPPTimeCurrent()
        clone.setComment("Cloned from run %d" % (pref_match.getRunID()))
        clone.setLastUserCurrent()
        clone.setJobID("")
        clone.setSubmitDir("")
        clone.setNotifyUser("")
        clone = rm.createRun(clone)
        if (clone == None):
            rm.rollbackTransaction()
            print "Failed inserting cloned run."
            return 1
        else:
            # Clone the RLS entries for the SGTs
            if (cloneLFNs(pref_match, clone, info.awp) != 0):
                rm.rollbackTransaction()
                print "Failed cloning LFNs for run %d." % (pref_match.getRunID())
                return 1
            else:
                rm.commitTransaction()
                pref_match = clone

    # Print the RunID to STDOUT
    print pref_match.getRunID()
    return 0


def cleanup():
    return 0
    
    
if __name__ == '__main__':
    if (init() != 0):
        sys.exit(1)
    retval = main()
    cleanup()
    sys.exit(retval)
    
