#!/usr/bin/env python3

import sys
import time
import os
sys.path.append("%s/RunManager" % os.path.dirname(os.path.realpath(__file__)))

from RunManager import *
from RC import *
from find_run import cloneLFNs

# Globals
class info:
    pass

MATCH_STATE_DICT = {"USE":["SGT Generated",], \
                        "CLONE":["PP Started", "PP Error", "Curves Generated", "Plotting", "Verified",]}


def init():
    global info
	
    # Get number of command-line arguments
    argc = len(sys.argv)

    if argc!=7:
        print("Usage: " + sys.argv[0] + " <site> [low-freq run id] [erf id] [rup var id] [merge frequency] [stochastic frequency]")
        return 1

    info.site = sys.argv[1]
    info.lf_run_id = int(sys.argv[2])
    info.erf = int(sys.argv[3])
    info.rup_var = int(sys.argv[4])
    info.merge_freq = float(sys.argv[5])
    info.stoch_freq = float(sys.argv[6])
    info.awp = True

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
    lf_run = rm.getRunByID(info.lf_run_id)
    #See if this run is in an OK state
    if lf_run.getStatus()!="Verified":
        print("Low-frequency run ID %d isn't in verified status, so it can't be used, aborting." % info.lf_run_id)
        return -1

    #Check that parameters match
    if lf_run.getERFID()!=info.erf or lf_run.getRupVarID()!=info.rup_var or lf_run.getSite().getShortName()!=info.site:
        print("Low-frequency parameters don't match stochastic parameters, aborting.")
        return -2

    #Low-frequency run seems good.  Use these parameters to search for a valid stochastic run.
    searchrun = Run()
    searchsite = Site()
    searchsite.setShortName(info.site)
    searchrun.setSite(searchsite)
    if (info.erf != None):
        searchrun.setERFID(info.erf)
        searchrun.setSGTVarID(lf_run.getSGTVarID())
        searchrun.setRupVarID(info.rup_var)
        searchrun.setVelID(lf_run.getVelID())
        searchrun.setMaxFreq(info.stoch_freq)
        searchrun.setLowFreqCutoff(info.merge_freq)
    
    # Find preferred runids, if any
    need_clone = False
    pref_match = findMatch(rm, searchrun, MATCH_STATE_DICT["USE"])
    if (pref_match == None):
        need_clone = True
        pref_match = findMatch(rm, searchrun, MATCH_STATE_DICT["CLONE"])

    if (pref_match == None):
        print("No matching runs found.")
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
            print("Failed inserting cloned run.")
            return 1
        else:
            # Clone the RLS entries for the SGTs
            if (cloneLFNs(pref_match, clone, info.awp) != 0):
                rm.rollbackTransaction()
                print("Failed cloning LFNs for run %d." % (pref_match.getRunID()), file=sys.stderr)
                return 1
            else:
                rm.commitTransaction()
                pref_match = clone

    # Print the RunID to STDOUT
    print(pref_match.getRunID())
    return 0



def cleanup():
    return 0


if __name__ == '__main__':
    if (init() != 0):
        sys.exit(1)
    retval = main()
    cleanup()
    sys.exit(retval)
