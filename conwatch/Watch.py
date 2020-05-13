#!/usr/bin/env python

import time
import sys
import subprocess
import sqlite3
from ConWatchConfig import *


# DAG information
jobid = "Unknown"
desc = "Unknown"
notify = "Unknown"
glideins = "N"


# DB information
conn = None
c = None


def isValidDAG(jobid):
    # Specify the condor_q command to run
    condorcmd = ['condor_q', '-long', jobid]
    
    # Under high scheduler load, will get failure to connect error. In this case
    # retry the query
    count = 0
    done = False
    while ((count < CONDOR_RETRY) and (not done)):
        print "Querying condor_q for job " + jobid
        p = subprocess.Popen(condorcmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        classad = p.communicate()[0]
        retcode = p.returncode
        if retcode != 0:
            print "condor_q failure, retcode=" + str(retcode)
            count = count + 1
            print "Waiting " + str(CONDOR_WAIT) + " secs, then retrying"
            time.sleep(CONDOR_WAIT)
        else:
            done = True
            
    if (not done):
        classad = []
    else:
        classad = classad.splitlines()
        
    # Check to ensure there is a classad, there is no parent DAG, and this is a dagman
    if (len(classad) <= 4):
        return False
    for ad in classad:
        tokens = ad.split(" = ", 1)
        if (tokens[0] == 'DAGManJobId'):
            return False
        if (tokens[0] == 'Cmd'):
            if (tokens[1].find(CONDOR_DAGMAN) == -1):
                return False
        
    return True


def init():
    global conn
    global c
    global jobid
    global desc
    global notify
    global glideins
    
    # Get number of command-line arguments
    argc = len(sys.argv)
    
    # Check command line arguments
    if (argc < 3):
        print "Usage: " + sys.argv[0] + " [-glideins] <condor job id> <job desc> <notify_user>"
        print "Example: " + sys.argv[0] + " 123123.0 \"USC pp workflow\" patrices@usc.edu"
        return 1

    # Parse command-line options
    for i in range(1, argc - 3):
        if (sys.argv[i] == "-glideins"):
            glideins = "Y"
            
    jobid = sys.argv[argc - 3].split(".")[0]
    desc = sys.argv[argc - 2]
    notify = sys.argv[argc - 1]
    
    print "Configuration:"
    print "Jobid:\t\t" + jobid
    print "Description:\t" + desc
    print "Notify:\t\t" + notify
    print "Glideins:\t" + glideins
    
    # Check that the specified jobid is valid
    if (not isValidDAG(jobid)):
        print "Jobid " + jobid + " is not a valid DAG"
        print "Must be top level DAG, currently in queue"
        return 1

    # Create connection
    try:
        conn = sqlite3.connect(DB_FILE)
    except:
        print sys.exc_info()
        return 1
    
    # Create cursor
    c = conn.cursor()
        
    return 0
    
    
def main():

    # Get current time
    curtime = int(time.time())

    # Check if DAG is already being monitored
    try:
        c.execute("select * from dags where jobid = ?", (jobid,))
        rows = c.fetchall()
        for r in rows:
            if (r[TABLE_DAG_POS_NOTIFY] == notify):
                print "DAG " + jobid + " is already being monitored by you"
                return 1
    except:
        print sys.exc_info() 
        return 1

    # Write DAG entry
    print "Adding new DAG to DB..."
    row = (None, jobid, desc, curtime, glideins, STATUS_INIT, notify, "",)
    try:
        c.execute("insert into dags values (?, ?, ?, ?, ?, ?, ?, ?)", row)
    except:
        print sys.exc_info()
        return 1
            
    # Save (commit) the changes
    conn.commit()
    
    # Show list of current DAGs
    print "Currently executing DAGs:"
    try:
        c.execute('select * from dags where not status = ?', (STATUS_DONE,))
        for row in c:
            print row
    except:
        print sys.exc_info()
        return 1
    
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
    
