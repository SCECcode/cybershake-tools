#!/usr/bin/env python


# Imports
from RunManager import *
from Mailer import *


# Global vars
class info:
    pass


def doSGT(args):
    global info

    stage = args[0]
    stages = ["preCVM", "vMeshGen", "vMeshMerge", "sgtGenXY", "sgtMergeXY"]
    subject = "Status - Run %d (%s) %s Workflow" % \
        (info.run_id, info.site, info.workflow) 
    msg = "Workflow is currently on this stage:\r\n\r\n"

    if (not stage in stages):
        print "Stage " + stage + " not found"
        return 1

    numstage = len(stages)
    istage = stages.index(stage)
    for i in range(0, numstage):
        if (i <= istage):
            msg = msg + stages[i] + ": Complete\r\n"
        else:
            msg = msg + stages[i] + ": Scheduled\r\n"
    
    # Send the email
    m = Mailer()
    m.send(info.notify_to, subject, msg)

    return 0


def doPP(args):
    global info

    stage = args[0]
    daxnum = int(args[1])
    maxdax = int(args[2])
    stages = ["CheckSgt", "DAX", "DBWrite"]
    subject = "Status - Run %d (%s) %s Workflow" % \
        (info.run_id, info.site, info.workflow) 
    msg = "Workflow is currently on this stage:\r\n\r\n"   
    
    if (not stage in stages):
        print "Stage " + stage + " not found"
        return 1

    if (daxnum > maxdax):
        print "DAX number %d is greater than max DAX %d" % (daxnum, maxdax)
        return 1

    numstage = len(stages)
    istage = stages.index(stage)
    for i in range(0, numstage):
        if (i <= istage):
            if (i == istage) and (i == 1):
                msg = msg + stages[i] + ": Number " + str(daxnum) + \
                    " of approx " + str(maxdax) + " completed successfully\r\n"
            else:
                msg = msg + stages[i] + ": Complete\r\n"
        else:
            msg = msg + stages[i] + ": Scheduled\r\n"

    # Send the email
    m = Mailer()
    m.send(info.notify_to, subject, msg)

    return 0


# Workflow definitions
# Mapping of workflow name -> tuple (number of arguments, handler)
WORKFLOWS = {"SGT":(1, doSGT), \
             "PP":(3, doPP)}


def init():
    global info

    # Get number of command-line arguments
    argc = len(sys.argv)
    
    # Parse command line arguments
    if (argc < 4):
        print "Usage: " + sys.argv[0] + " <run_id> <workflow> <stage info>"
        print "Example: " + sys.argv[0] + " 213 SGT preCVM"
        print "Example: " + sys.argv[0] + " 376 PP CheckSgt 12 80"
        return 1
            
    info.run_id = int(sys.argv[1])
    info.workflow = sys.argv[2]
    info.stage_info = sys.argv[3:]

    print "Configuration:"
    print "Site:\t\t" + str(info.run_id)
    print "Workflow:\t" + info.workflow
    print "Stage Info:\t" + str(info.stage_info)

    # Check that the workflow is valid and the correct number of 
    # arguments were supplied
    try:
        wfinfo = WORKFLOWS[info.workflow]
        numargs = wfinfo[0]
        if (len(info.stage_info) != numargs):
            print "Workflow " + info.workflow + \
                " requires " + str(numargs) + " argument(s)"
            return 1
    except:
        print "Unable to find " + info.workflow
        return 1        
    
    # Load the notify list from the DB
    rm = RunManager(readonly=True)
    rm.useHTML(False)

    run = rm.getRunByID(info.run_id)
    if (run == None):
        print "Failed to retrieve run %d from DB." (info.run_id)
        return 1

    print "Notification List:"
    info.notify_to = run.getNotifyUserAsList()
    if ((info.notify_to == None) or (len(info.notify_to) == 0)):
        print " No users specified - notifications disabled"
        info.notify_to = []
    else:
        for n in info.notify_to:
            if ((n == None) or (n == "")):
                print " Invalid user list specified - notifications disabled"
                info.notify_to = []
                break
            else:
                print " " + n

    # Save site name
    info.site = run.getSiteName()

    return 0


def main():
    global info

    if (len(info.notify_to) > 0):
        # Execute the workflow handler
        retcode = WORKFLOWS[info.workflow][1](info.stage_info)
        if (retcode != 0):
            print "Error sending email notification"
            return 1

    return 0


def cleanup():
    return 0


if __name__ == '__main__':
    if (init() != 0):
        sys.exit(1)
    if (main() != 0):
        sys.exit(1)
    cleanup()
    sys.exit(0)
