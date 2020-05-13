#!/usr/bin/env python

import sys
import os
import pwd
import socket
import sqlite3
import subprocess
import smtplib
import time
from ConWatchConfig import *


# Constants
NO_PARENT = "-1"
NOTIFY_FROM = "cybershk@shock.usc.edu"
NUM_SECS_IN_TWOWEEKS = 1209600
NUM_SECS_IN_SIXHOURS = 21600

# Column order for table returned by condor_q
LIST_POS_JOBID = 0
LIST_POS_CMD = 1
LIST_POS_STATUS = 2
LIST_POS_STATUS_TIME = 3
LIST_POS_RESTARTS = 4
LIST_POS_DAGID = 5

# Column order for internal job map
MAP_POS_CMD = 0
MAP_POS_STATUS = 1
MAP_POS_STATUS_TIME = 2
MAP_POS_RESTARTS = 3
MAP_POS_HAS_ERROR = 4
MAP_POS_CHILD = 5

# condor_q status values
UNEXPANDED = "0"
IDLE = "1"
RUNNING = "2"
REMOVED = "3"
COMPLETED = "4"
HELD = "5"
SUBMISSION_ERR = "6"


# Global vars
conn = None
c = None


def init():
    global NOTIFY_FROM
    global DB_FILE
    global conn
    global c
    
    # Get number of arguments
    argc = len(sys.argv)
    
    # Check command line arguments
    if (argc == 2):
        DB_FILE = sys.argv[1]
    elif (argc > 2):
        print "Usage: " + sys.argv[0] + " [DB file]"
        print "Example: " + sys.argv[0] + " conwatch.db"
        return 1
    
    print "Configuration:"
    print "DB File:\t\t" + DB_FILE
    
    # Create DB connection
    conn = sqlite3.connect(DB_FILE)

    # Create cursor
    c = conn.cursor()
    
    # Get the current user id
    userid = pwd.getpwuid(os.getuid())[0]
    domain = socket.getfqdn()
    NOTIFY_FROM = userid + "@" + domain
        
    return 0


# Send email msg using SMTP
def sendNotification(dagid, desc, msg, notify_user):
    msg = "From: " + NOTIFY_FROM + "\r\nTo: " + notify_user + "\r\nSubject: Status - " \
        + desc + "\r\n" + msg + \
        "\r\n---------------------------------------------------\r\nAutomated msg from Condor Watch\r\n"
    server = smtplib.SMTP('localhost')
    #server.set_debuglevel(1)
    server.sendmail(NOTIFY_FROM, notify_user, msg)
    server.quit()

    return 0


# Return list of children belonging to dagid from the jobmap
def getChildren(dagid, jobmap):
    children = []

    # Identify children of dagid
    try:
        children = jobmap[dagid][MAP_POS_CHILD]
    except KeyError:
        # This is an error
        children = []

    i = 0
    oldi = 0
    oldlen = len(children)
    while(oldi < oldlen):
        for i in range(oldi,oldlen):
            newchildren = jobmap[children[i]][MAP_POS_CHILD]
            if (len(newchildren) > 0):
                children = children + newchildren
        # Need to increment to next child that was potentially appended to list
        oldi = i + 1
        oldlen = len(children)
                
    return children


# Convert a job list into a job map, keyed by job id
def getJobMap(joblist):
    childmap = {}

    # Identify children of dagid
    for j in joblist:
        jobid = j[LIST_POS_JOBID]
        cmd = j[LIST_POS_CMD]
        jobstatus = j[LIST_POS_STATUS]
        enteredstatus = int(j[LIST_POS_STATUS_TIME])
        # Convert # job starts to # restarts
        restarts = int(j[LIST_POS_RESTARTS])
        if (restarts >= 1):
            restarts = restarts - 1;
        dagid = j[LIST_POS_DAGID]
        keys = childmap.keys()
        if (not (jobid in keys)):
            childmap[jobid] = [cmd, jobstatus, enteredstatus, restarts, 'N', []]
        else:
            # Update job info, this jobid probably represents a previously inserted DAG id
            # which had placeholders
            childmap[jobid][MAP_POS_CMD] = cmd
            childmap[jobid][MAP_POS_STATUS] = jobstatus
            childmap[jobid][MAP_POS_STATUS_TIME] = enteredstatus
            childmap[jobid][MAP_POS_RESTARTS] = restarts

        if (dagid != NO_PARENT):
            if (not (dagid in keys)):
                childmap[dagid] = [None, None, None, None, 'N', [jobid]]
            else:
                childmap[dagid][MAP_POS_CHILD].append(jobid)
    
    return childmap


# Query condor_q for list of currently active jobs
def getJobList():
    # Specify the condor_q command to run
    condorcmd = ['condor_q', '-format', '\n%d ', 'ClusterId', \
                    '-format', '%s ', 'Cmd', \
                    '-format', '%d ', 'JobStatus', \
                    '-format', '%d ', 'EnteredCurrentStatus', \
                    '-format', '%d ', 'NumJobStarts', \
                    '-format', '%d', 'DAGManJobId']

    # Under high scheduler load, will get failure to connect error. In this case
    # retry the query
    count = 0
    done = False
    while ((count < CONDOR_RETRY) and (not done)):
        print "Querying condor_q for all jobs"
        p = subprocess.Popen(condorcmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        joblist = p.communicate()[0]
        retcode = p.returncode
        if retcode != 0:
            print "condor_q failure, retcode=" + str(retcode)
            count = count + 1
            time.sleep(CONDOR_WAIT)
        else:
            done = True
            
    if (not done):
        joblist = []
        return (1, joblist)
    else:
        joblist = joblist.splitlines()
    
    # Parse this job list into a useful format
    parsedlist = []  
    for j in joblist:
        row = j.split()
        #print row
        if (len(row) == LIST_POS_DAGID):
            row.append(NO_PARENT)
            parsedlist.append(row)
        elif (len(row) == LIST_POS_DAGID + 1):
            parsedlist.append(row)
        # Ignore empty/incorrectly formatted lines (should only have empty lines)
        else:
            print "Warning: Incorrectly formatted condor_q row: " + str(row)
            
    print "Found " + str(len(parsedlist)) + " jobs"
        
    return (0, parsedlist)


# Delete old dags from the DB
# CASCADE DELETE is not implemented in this version of SQLite
def deleteOldDAGs():    
    # Calculate cut-off time
    deltime = int(time.time()) - (NUM_SECS_IN_TWOWEEKS)
    
    # Retrieve list of old DAGs
    try:
        c.execute('select dagid from dags where submit_time < ?', (deltime,))
        daglist = c.fetchall()
        print "Found " + str(len(daglist)) + " old DAGs to delete"
        c.executemany('delete from jobs where dagid = ?', daglist)
        c.executemany('delete from dags where dagid = ?', daglist)
    except:
        print "Unable to delete old DAGs from DB"
        print sys.exc_info()
        return 1

    # Commit changes
    conn.commit()   
    
    return 0


# Get tracked dags from the DB
def getDAGs():
    try:
        # Get list of current DAGs
        c.execute('select * from dags where not status = ?', (STATUS_DONE,))
        rows = c.fetchall()
    
        # Convert to list
        daglist = list(rows)
        
        return daglist
    except:
        print "Unable to retrieve DAGs from DB"
        print sys.exc_info()
        return []


# Save changes to dags in the DB
def saveDAGs(daglist):
    print "Updating dags in DB"
    numupdate = 0
    for dag in daglist:
        dagid = dag[TABLE_DAG_POS_DAGID]
        jobid = dag[TABLE_DAG_POS_JOBID]
        status = dag[TABLE_DAG_POS_STATUS]
        errors = dag[TABLE_DAG_POS_ERRORS]
        notify = dag[TABLE_DAG_POS_NOTIFY]
        try:
            # Save changes to current DAGs
            c.execute('update dags set status = ?, errors = ? where dagid = ?', (status, errors, dagid,))
            numupdate = numupdate + 1
        except:
            print "Unable to update dags table in DB for DAG " + jobid + ":" + notify
            print sys.exc_info()
 
    print "Updated " + str(numupdate) + " rows"
    
    # Commit changes
    conn.commit()   
     
    return 0


# Save snap-shot of job list to the DB
def saveJobs(daglist, jobmap):
    
    # Get current system time
    curtime = int(time.time())
    
    print "Saving error jobs in DB"
    numinsert = 0
    for dag in daglist:
        dagid = dag[TABLE_DAG_POS_DAGID]
        jobid = dag[TABLE_DAG_POS_JOBID]
        notify = dag[TABLE_DAG_POS_NOTIFY]
        
        # Find this dag's children
        children = getChildren(jobid, jobmap)

        # Construct list of data to insert
        insertlist = []
        for j in children:
            if (jobmap[j][MAP_POS_HAS_ERROR] == 'Y'):
                if (jobmap[j][MAP_POS_CMD].find(CONDOR_DAGMAN) != -1):
                    is_dag = 'Y'
                else:
                    is_dag = 'N'       
                row = (None, j, dagid, jobmap[j][MAP_POS_CMD], is_dag, \
                       jobmap[j][MAP_POS_HAS_ERROR], jobmap[j][MAP_POS_STATUS], \
                       jobmap[j][MAP_POS_STATUS_TIME], jobmap[j][MAP_POS_RESTARTS], curtime,)
                insertlist.append(row)
        if (len(insertlist) > 0):
            # Perform bulk insert
            try:
                c.executemany("insert into jobs values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", insertlist)
                numinsert = numinsert + len(insertlist)
            except:
                print "Unable to save jobs for DAG " + jobid + ":" + notify
                print sys.exc_info()
                
    
    print "Inserted " + str(numinsert) + " rows"

    # Commit changes
    conn.commit()   
     
    return 0


# Returns True if IDLE or RUNNING glideins exist
def haveGlideins(jobmap):
    # Verify at least one is I,R status in the condor queue
    
    for k in jobmap.keys():
        cmd = jobmap[k][MAP_POS_CMD]
        status = jobmap[k][MAP_POS_STATUS]
        if (cmd != None):
            if (cmd.find(GLIDEIN_CMD) != -1):
                if ((status == IDLE) or (status == RUNNING)):
                    return True
        
    return False


# Checks job status for each DAG
def checkStatus(daglist, jobmap):
    newrow = []
    newdaglist = []
    
    # Discover if glideins are present
    have_glideins = haveGlideins(jobmap)
    
    # Get current system time
    curtime = int(time.time())
    
    print "Checking status of DAGs"
    for row in daglist:
        errmsg = ""
        newrow = list(row)
        
        # Extract dag info from row
        dagid = int(row[TABLE_DAG_POS_DAGID])
        jobid = row[TABLE_DAG_POS_JOBID]
        desc = row[TABLE_DAG_POS_DESC]
        submit_time = int(row[TABLE_DAG_POS_SUBMIT_TIME])
        glidein = row[TABLE_DAG_POS_GLIDEIN]
        status = row[TABLE_DAG_POS_STATUS]
        notify = row[TABLE_DAG_POS_NOTIFY]
        errors = row[TABLE_DAG_POS_ERRORS]
        
        # Find this dag's children
        children = getChildren(jobid, jobmap)
        #print "DAG " + dagid + ": Found " + str(len(children)) + " children"
        
        # Check that this DAG exists in the queue
        keys = jobmap.keys()
        if (not jobid in keys):
                print "DAG " + jobid + ": not currently in queue - marking completed"
                errmsg = errmsg + "DAG " + jobid + ": not currently in queue - marking completed\n"
                newrow[TABLE_DAG_POS_STATUS] = STATUS_DONE
        else:                
            # If this job needs glideins, check that they are running
            if (glidein == 'Y'):
                print "DAG " + jobid + ": Checking for glideins"
                if (not have_glideins):
                    print "DAG " + jobid + ": No running glideins found"
                    errmsg = errmsg + "DAG " + jobid + ": No running glideins found\n"

            # Pull out statistics from jobmap
            errjids = []
            idlejids = []
            restartjids = []
            numnotdag = 0
            for j in children:
                # Extract needed data fields
                cstatus = jobmap[j][MAP_POS_STATUS]
                status_time = jobmap[j][MAP_POS_STATUS_TIME]
                restarts = jobmap[j][MAP_POS_RESTARTS]
                if (jobmap[j][MAP_POS_CMD].find(CONDOR_DAGMAN) != -1):
                    is_dag = 'Y'
                else:
                    is_dag = 'N'
                    numnotdag = numnotdag + 1       

                # Children in errors status
                if ((cstatus != IDLE) and (cstatus != RUNNING) and (cstatus != COMPLETED)):
                    errjids.append(j)
                    jobmap[j][MAP_POS_HAS_ERROR] = 'Y'
                # Non-DAG children idle for more than 6 hours
                diff = curtime - status_time
                if ((cstatus == IDLE) and (diff >= NUM_SECS_IN_SIXHOURS) and (is_dag == 'N')): 
                    idlejids.append(c)
                    # Not really an error
                    #jobmap[j][MAP_POS_HAS_ERROR] = 'Y'
                # Children that have restarts
                if (restarts > 0):
                    restartjids.append(j)
                    jobmap[j][MAP_POS_HAS_ERROR] = 'Y'

            # Check that all jobs associated with DAG are in good state (1, 2)
            print "DAG " + jobid + ": Checking for jobs in error status"
            print "DAG " + jobid + ": Jobs in error status, count=" + str(len(errjids))
            if (len(errjids) > 0):
                errmsg = errmsg + "DAG " + jobid + ": Children in error status:\n"
                i = 0
                for j in errjids:
                    errmsg = errmsg + " " + j
                    i = i + 1
                    if ((i % 12 == 0) or (i == len(errjids))):
                        errmsg = errmsg + "\r\n"

            # Check that there is at least one non-dag child running for this workflow
            print "DAG " + jobid + ": Checking for presence of non-DAG jobs"
            if (numnotdag == 0):
                print "DAG " + jobid + ": No non-DAGs running for this workflow"
                errmsg = errmsg + "DAG " + jobid + ": No non-DAGs running for this workflow\n"
            else:
                print "DAG " + jobid + ": Non-DAG jobs found, count=" + str(numnotdag)
                # Check that the jobs in the DAG haven't been stuck in the same state for too long.
                # This does not necessarily indicate an error condition - the job may legitimately be
                # waiting for computing resources to become available
                print "DAG " + jobid + ": Checking for jobs stuck in IDLE state"
                if (len(idlejids) == numnotdag):
                    print "DAG " + jobid + ": All jobs IDLE for > 6hrs"
                    errmsg = errmsg + "DAG " + jobid + ": All jobs IDLE for > 6hrs\n" 
                else:
                    print "DAG " + jobid + ": Jobs idle for > 6hrs, count=" + str(len(idlejids))

            # Check that there are no restarts
            print "DAG " + jobid + ": Checking for jobs with restarts"
            print "DAG " + jobid + ": Jobs with restarts, count=" + str(len(restartjids))
            if (len(restartjids) > 0):
                errmsg = errmsg + "DAG " + jobid + ": Children with restarts:\n"
                i = 0
                for j in restartjids:
                    errmsg = errmsg + " " + j
                    i = i + 1
                    if ((i % 12 == 0) or (i == len(restartjids))):
                        errmsg = errmsg + "\r\n"

        # Compare old and new error list for this DAG. If different, send email. Otherwise,
        # if this is a new job, send email
        if ((errors != errmsg) and (errmsg != "")):
            print "DAG " + jobid + ": Error list has changed since previous check"
            sendNotification(jobid, desc, errmsg, notify)

        # Update new row with new error list, and move new DAGs to run state
        newrow[TABLE_DAG_POS_ERRORS] = errmsg
        
        # The DAG is currently running, otherwise would have been marked STATUS_DONE
        if (newrow[TABLE_DAG_POS_STATUS] == STATUS_INIT):
            newrow[TABLE_DAG_POS_STATUS] = STATUS_RUN
            # If not already in error, send a confirmation email
            if (errmsg == ""):
                sendNotification(jobid, desc, "DAG " + jobid + ": Now being tracked by Condor Watch\n", notify)        
                pass

        # Save this row to the modified dag list
        newdaglist.append(newrow)

    return (newdaglist, jobmap)

    
def main():

    # Delete records older than 2 weeks
    deleteOldDAGs()

    # Get list of current DAGs and check their status
    daglist = getDAGs()
    print "Found " + str(len(daglist)) + " DAGs to monitor"
    for row in daglist:
        print row
        
    if (len(daglist) == 0):
        print "No work to do - exiting"
        return 0

    # Get list of current condor jobs
    (retcode, joblist) = getJobList();
    if (retcode != 0):
        print "Fatal error querying condor for job list"
        return 1
            
    # Check that job information exists, print warning but continue
    if (len(joblist) == 0):    
        print "Unable to find any jobs in queue"
        pass
    
    # Convert into a dictionary mapping jobs to status info and children
    jobmap = getJobMap(joblist)
    
    # Check that conversion to job map successful, print warning but continue
    if (len(jobmap.keys()) == 0):    
        print "Unable to find any jobs in map"
        pass
    
    # Check status of each dag
    (daglist, jobmap) = checkStatus(daglist, jobmap)
   
    # Save (possibly) modified DAG list to DB 
    saveDAGs(daglist)
    
    # Save snap-shot of jobs in error state
    saveJobs(daglist, jobmap)
    
    return 0    


def cleanup():
    # Close connection
    if (c != None):
        c.close()

    return 0
    
    
if __name__ == '__main__':
    init()
    main()
    cleanup()
    sys.exit(0)
