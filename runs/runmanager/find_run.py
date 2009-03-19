#!/usr/bin/env python

# Add RunManager modules to PYTHONPATH
import sys
import time
import subprocess
sys.path.append('/home/scec-00/patrices/code/trunk/RunManager/')


# General imports
from RunManager import *

# Constants
RLS_HOST = "rls://shock.usc.edu"


# Globals
class info:
    pass
SEARCH_LIST = ["SGT Generated", "PP Started", "PP Error", "Curves Generated",]


def init():
    global info
    
    # Get number of command-line arguments
    argc = len(sys.argv)
    
    # Check command line arguments
    if ((argc != 2) and (argc != 5)):
        print "Usage: " + sys.argv[0] + " <site> [erf id] [sgt var id] [rup var id]"
        print "Example: " + sys.argv[0] + " USC 35 5 3"
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


def runCommand(cmd):
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, \
                                 stderr=subprocess.STDOUT)
        output = p.communicate()[0]
        retcode = p.returncode
        if retcode != 0:
            #print output
            #print "Non-zero exit code"
            return None
    except:
        #print sys.exc_info()
        #print "Failed to run cmd: " + str(cmd)
        return None

    output = output.splitlines()
    return output


def getPFN(lfn):
    cmd = ['globus-rls-cli',  'query',  'lrc', 'lfn',  lfn, RLS_HOST,]
    output = runCommand(cmd)
    if (output == None):
        print "LFN %s not found" % (lfn)
        return None
    else:
        pfn_list = []
        for l in output:
            tokens = l.split(":", 1)
            if (len(tokens) != 2):
                print "Unexpected result: %s" % (str(output))
                return None
            pfn = tokens[1]
            pfn = pfn.strip()
            pfn_list.append(pfn)
        return pfn_list


def getPool(pfn):
    cmd = ['globus-rls-cli', 'attribute', 'query', pfn, 'pool', 'pfn', RLS_HOST,]
    output = runCommand(cmd)
    if (output == None):
        print "PFN %s not found" % (pfn)
        return None
    else:
        pool_list = []
        for l in output:
            tokens = l.split(":", 2)
            if (len(tokens) != 3):
                print "Unexpected result: %s" % (str(output))
                return None
            pool = tokens[2]
            pool = pool.strip()
            pool_list.append(pool)
        return pool_list


def createLFN(lfn, pfn, pool):
    cmd = ['globus-rls-cli', 'create', lfn, pfn, RLS_HOST]
    output = runCommand(cmd)
    if (output == None):
        print "Failed to create LFN %s" % (lfn)
        return 1

    # Pool attribute for pfn should already exist
    #else:
        #cmd = ['globus-rls-cli', 'attribute', 'add', pfn, 'pool', 'pfn', 'string', pool, RLS_HOST]
        #print cmd
        #output = runCommand(cmd)
        #if (output == None):
            #print "Failed to create pool attribute %s for PFN" % (pool)
            #return 1

    return 0


def cloneLFNs(match, clone):
    
    lfns = ["%s_%d_fx.sgt", \
                "%s_%d_fy.sgt", \
                "%s_%d_fx.sgt.md5", \
                "%s_%d_fy.sgt.md5", ]

    for lfn in lfns:
        old_lfn = lfn % (match.getSiteName(), match.getRunID())
        new_lfn = lfn % (clone.getSiteName(), clone.getRunID())

        # Get PFN associated with this LFN
        pfn_list = getPFN(old_lfn)
        if (pfn_list == None):
            return 1

        # Take the first pfn in the list
        pfn = pfn_list[0]

        # Get this PFNs pool attributes
        pool_list = getPool(pfn)
        if ((pool_list == None) or (len(pool_list) == 0)):
            # Not the end of the world if no pool attribute
            pool = None
        else:
            # Take the first pool attribute in the list
            pool = pool_list[0]

        #print "pfn=%s, pool=%s" % (pfn, str(pool))

        # Create new LFN for cloned run
        if (createLFN(new_lfn, pfn, pool) != 0):
            return 1

    return 0


def main():
    global info

    rm = RunManager(readonly=False)
    rm.useHTML(False)

    searchrun = Run()
    searchrun.setSiteName(info.site)
    if (info.erf != None):
        searchrun.setERFID(info.erf)
        searchrun.setSGTVarID(info.sgt_var)
        searchrun.setRupVarID(info.rup_var)

    pref_match = None

    for s in SEARCH_LIST:
        searchrun.setStatus(s)
        matches = rm.getRuns(searchrun, lock=False)

        if (matches != None):
            for run in matches:
                sgt_time = int(time.mktime(time.strptime(run.getSGTTime(), '%Y-%m-%d %H:%M:%S')))
                if (pref_match == None):
                    pref_match = run
                else:
                    pref_time = int(time.mktime(time.strptime(pref_match.getSGTTime(), '%Y-%m-%d %H:%M:%S')))
                    if (sgt_time > pref_time):
                        pref_match = run
                        
            if (pref_match != None):
                if ((s == "SGT Generated") or (s == "Curves Generated")):
                    # Found an acceptable match
                    break
                else:
                    # Keep checking if state = PP Started/PP Error because we want to find latest
                    # run of all the PP Started/PP Error/Curves Generated
                    pass
            else:
                pass

    if (pref_match == None):
        print "No matching runs found."
        return 1
    
    #print "Preferred match:"
    #pref_match.dumpToScreen()
        
    if (pref_match.getStatus() != "SGT Generated"):
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
        clone = rm.createRun(clone)
        if (clone == None):
            rm.rollbackTransaction()
            print "Failed inserting cloned run."
            return 1
        else:
            # Clone the RLS entries for SGTs
            if (cloneLFNs(pref_match, clone) != 0):
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
    
