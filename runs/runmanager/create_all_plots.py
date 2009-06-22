#!/usr/bin/env python

# Add RunManager modules to PYTHONPATH
import sys
sys.path.append('/home/scec-00/patrices/code/trunk/RunManager/')


# General imports
import os
import time
import subprocess
from Config import *
from RunManager import *


# Globals
class info:
    pass


def init():
    global info

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


def getCompletedRuns(rm):

    state_list = [DONE_STATE,]

    runs = rm.getRunsByState(state_list)
    if ((runs == None) or (len(runs) == 0)):
        print "No completed runs found in DB!"
        return None

    # Uncomment to generate plots for specific run ids
    #filter_list = [205, 210, 211, 209, 208, 207, 212, 213, 214, 206]
    #return_runs = []
    #for run in runs:
    #    if (run.getRunID() in filter_list):
    #        return_runs.append(run)
    #if (len(return_runs) == 0):
    #    return None

    return runs



def createPlots(runs):

    for run in runs:

            # Construct paths
            src_dir = '%s%s' % (CURVE_DIR, run.getSite().getShortName())

            # Run the plotter
            print "Running plotter for site %s" % (run.getSite().getShortName())
            os.chdir(OPENSHA_DIR)
            plot_cmd = [OPENSHA_CURVE_SCRIPT, \
                            '-R', '%d' % (run.getRunID()), \
                            '-s', '%s' % (run.getSite().getShortName()), \
                            '-n', \
                            '-o', src_dir, \
                            '-ef', OPENSHA_ERF_XML, \
                            '-af', OPENSHA_AF_XML, \
                            '-t', 'png,pdf', \
                            '-pf', OPENSHA_DBPASS_FILE, \
                            '-p', '2,3,5,10',]

            print plot_cmd
            results = runCommand(plot_cmd)
            if (results == None):
                print "Failed to run plotter."
            else:
                print "Plotting successful"

    return 0


def main():

    print "-- Starting up at %s --" % (time.strftime("%Y-%m-%d %H:%M:%S"))

    rm = RunManager(readonly=True)
    rm.useHTML(False)

    runs = getCompletedRuns(rm)
    if (runs != None):
        print "Found %d runs" % (len(runs))
        createPlots(runs)

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
    
