#!/usr/bin/env python3


# DB File location
DB_FILE = '/home/shock/scottcal/runs/cybershake-tools/conwatch/conwatch.db'

# Condor retry attempts
CONDOR_RETRY = 5
CONDOR_WAIT = 5

# Command names for condor dagman and glideins
GLIDEIN_CMD = "glidein_run"
CONDOR_DAGMAN = "condor_dagman"

# Valid dags.status column values 
STATUS_INIT = "INITIAL"
STATUS_RUN = "RUNNING"
STATUS_DONE = "DONE"

# Column order to DB dag table
TABLE_DAG_POS_DAGID = 0
TABLE_DAG_POS_JOBID = 1
TABLE_DAG_POS_DESC = 2
TABLE_DAG_POS_SUBMIT_TIME = 3
TABLE_DAG_POS_GLIDEIN = 4
TABLE_DAG_POS_STATUS = 5
TABLE_DAG_POS_NOTIFY = 6
TABLE_DAG_POS_ERRORS = 7
