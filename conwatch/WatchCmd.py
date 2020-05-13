#!/usr/bin/env python

import time
import sys
import subprocess
import sqlite3
from ConWatchConfig import *


# DAG information
cmd = ""
cmdargs = []

# DB information
conn = None
c = None


def listDAGs(cmdargs):
    # Specify the condor_q command to run
    condorcmd = ['condor_q', '-format', '\n%d ', 'ClusterId', \
                 '-format', '%s ', 'Cmd', \
                 '-format', '%s ', 'Iwd', \
                 '-format', '%d', 'DAGManJobId']
    
    # Under high scheduler load, will get failure to connect error. In this case
    # retry the query
    count = 0
    done = False
    while ((count < CONDOR_RETRY) and (not done)):
        print "Querying condor_q for DAGS"
        p = subprocess.Popen(condorcmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        joblist = p.communicate()[0]
        retcode = p.returncode
        if retcode != 0:
            print "condor_q failure, retcode=" + str(retcode)
            count = count + 1
            print "Waiting " + CONDOR_WAIT + " secs, then retrying"
            time.sleep(CONDOR_WAIT)
        else:
            done = True
            
    if (not done):
        joblist = []
    else:
        joblist = joblist.splitlines()
        
    print "Top-level DAGs running in Condor:"
    found = 0
    for j in joblist:
        tokens = j.split()
        if ((len(tokens) == 3) and (tokens[1].find(CONDOR_DAGMAN) != -1)):
            print tokens[0], tokens[2]
            found = found + 1
            
    if (found == 0):
        print "None found."
        
    return 0


def listWatchList(cmdargs):
    try:
        c.execute("select * from dags where not status = ?", (STATUS_DONE,))
        daglist = c.fetchall()
        print "DAGs in Watch List:"
        if (len(daglist) == 0):
            print "No DAGs in watch list"
        else:
            for row in daglist:
                print row
 
    except:
        print sys.exc_info() 
        return 1
    
    return 0


def showErrors(cmdargs):
    # Trim the .# if the user specified it 
    jobid = cmdargs[0].split(".")[0]
    notify = cmdargs[1]    
    
    try:
        print "Finding DAGs for jobid " + jobid + ":" + notify
        c.execute("select dagid from dags where jobid = ? and notify_user = ?", (jobid, notify,))
        daglist = c.fetchall()
        if (len(daglist) == 0):
            print "No DAGs found for jobid " + jobid + ":" + notify
            return 1
        else:
            # dagid is first field
            for dag in daglist:
                dagid = dag[0]
                print "Errors for dagid " + str(dagid) + " (" + jobid + ":" + notify + ")"
                print "--------------------------------------------------------------"
                c.execute('select * from jobs where dagid = ?', (dagid,))
                errorlist = c.fetchall()
                for error in errorlist:
                    print error
                if (len(errorlist) == 0):
                    print "No errors found"
    except:
        print "Unable to list errors for DAG " + jobid + ":" + notify
        print sys.exc_info() 
        return 1
 
    # Save (commit) the changes
    conn.commit()
       
    return 0


def removeDAG(cmdargs):
    # Trim the .# if the user specified it 
    jobid = cmdargs[0].split(".")[0]
    notify = cmdargs[1]
    
    try:
        print "Finding all DAGs for jobid " + jobid + ":" + notify
        c.execute("select dagid from dags where jobid = ? and notify_user = ?", (jobid, notify,))
        daglist = c.fetchall()
        if (len(daglist) == 0):
            print "No DAGs found for jobid " + jobid + ":" + notify
            return 1
        else:
            print "Removing all DAGs with jobid " + jobid + ":" + notify
            # Note that there might be multiple rows since a job may have been submitted 
            # to Condor Watch more than once (in the case of a rescue DAG)
            for dag in daglist:
                # dagid is first field.
                dagid = dag[0]
                print "Deleting entries for dagid " + str(dagid)
                c.execute('delete from jobs where dagid = ?', (dagid,))
                c.execute('delete from dags where dagid = ?', (dagid,))
    except:
        print "Unable to remove DAGs for jobid " + jobid + ":" + notify
        print sys.exc_info() 
        return 1
 
    # Save (commit) the changes
    conn.commit()
 
    return 0


# Constants
VALID_COMMANDS = {"-condordags":listDAGs, \
                  "-watchlist":listWatchList, \
                  "-errors":showErrors, \
                  "-removedag":removeDAG}


def init():
    global conn
    global c
    global cmd
    global cmdargs
    
    # Get number of command-line arguments
    argc = len(sys.argv)
    
    # Check command line arguments
    if (argc < 2):
        print "Usage: " + sys.argv[0] + " <-command> [command arguments]"
        print "Supported commands:"
        print "\t-condordags"
        print "\t-watchlist"
        print "\t-errors <dagid> <user>"
        print "\t-removedag <dagid> <user>"
        return 1

    # Parse command-line options
    cmd = sys.argv[1]
    if (argc >= 2):
        cmdargs = sys.argv[2: argc]
    else:
        cmdargs = []
        
    validcmds = VALID_COMMANDS.keys()
    if (not cmd in validcmds):
        print "Invalid command specified"
        return 1
    
    print "Command: " + cmd
    print "Command Args: " + str(cmdargs)
 
    if ((cmd == "-errors")or (cmd == "-removedag")):
        if (len(cmdargs) != 2):
            print "Jobid and user must be specified"
            return 1
    
    # Create DB connection
    try:
        conn = sqlite3.connect(DB_FILE)
    except:
        print sys.exc_info()
        return 1
    
    # Create cursor
    c = conn.cursor()
    
    return 0


def main():
    # Use the dictionary to lookup the proper function given the command
    f = VALID_COMMANDS[cmd]
    f(cmdargs)
        
    return 0


def cleanup():
    # Close connection
    if (c != None):
        c.close()
    
    return 0
    
    
if __name__ == '__main__':
    if (init() != 0):
        sys.exit(1)
    main()
    cleanup()
    sys.exit(0)
