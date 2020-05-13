#!/usr/bin/env python

import sqlite3
import sys
from ConWatchConfig import *


# Create connection
try:
    conn = sqlite3.connect(DB_FILE)
except:
    print sys.exc_info()
    sys.exit(1)

# Create cursor
c = conn.cursor()

# Delete any existing tables
print "Dropping current schema"
try:
    c.execute('drop table dags');
except:
    print sys.exc_info()
    
try:
    c.execute('drop table jobs');
except:
    print sys.exc_info()

# Save (commit) the changes
conn.commit()

# Create tables
print "Creating tables"

# Table: dags
# Columns: dagid, unique integer
#          jobid, associated condor job ID
#          desc, user-specified description of dag
#          submit_time, time user submitted dag to Condor Watch
#          glideins, 'Y'/'N' if glideins are necessary for this DAG
#          status, current status of DAG - starts at 'INITIAL' if new
#          notify_user, the email address for notifications
#          errors, list of errors known for this dag
#
try:
    c.execute('''create table dags (dagid integer primary key, jobid text NOT NULL, desc text, \
        submit_time integer, glideins text, status text, notify_user text, errors text)''')
except:
    print sys.exc_info()
    sys.exit(1)

# Table: jobs
# Columns: jobid, unique integer
#          condorjobid, condor job id
#          dagid, foreign key to dags.dagid. Stores the top level parent DAG only
#          cmd, job name (cmd from condor)
#          is_dag, if cmd=condor_dagman then 'Y', 'N' otherwise
#          has_error, if flagged in an error state
#          status, condor job status
#          status_time, time entered status
#          num_restarts, number of times this job has been restarted
#          save_time, time record saved
#
# Note that foreign keys are not currently supported in SQLite, so the constraint is parsed
# but now enforced.
#
try:
    c.execute('''create table jobs (jobid integer primary key, condorjobid text, \
        dagid integer NOT NULL constraint DAGKEY references dags(dagid) on delete cascade, \
        cmd text, is_dag text, has_error text, status text, status_time integer, \
        num_restarts integer, save_time integer)''')
except:
    print sys.exc_info()
    sys.exit(1)


# Save (commit) the changes
conn.commit()

    
# Table join
#print "Table join"
#c.execute('select * from dags, jobs where jobs.jobid = ? and jobs.dagid = dags.dagid', ('123.0',))
#for row in c:
#    print row


# Close DB connection
c.close()

sys.exit(0)
