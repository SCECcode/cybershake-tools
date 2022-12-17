#!/usr/bin/env python3

import sys
import os
import pymysql
import datetime

if len(sys.argv)<2:
    print("Usage: %s <ID to clone>" % (sys.argv[0]))
    sys.exit(1)

old_id=int(sys.argv[1])

with open("/home/shock/scottcal/runs/config/db_pass.txt", "r") as fp_in:
    line = fp_in.readline()
    (user, passwd) = line.split(":")
    fp_in.close()
conn = pymysql.connect(user=user.strip(), passwd=passwd.strip(), host="moment.usc.edu", db="CyberShake")
cur = conn.cursor()

query = "select Site_ID,ERF_ID,SGT_Variation_ID,Rup_Var_Scenario_ID,SGT_Host,PP_Host,SGT_Time,PP_Time,Status_Time,Last_User,Notify_User,Velocity_Model_ID,Max_Frequency,Low_Frequency_Cutoff,Model_Vs30,Mesh_Vsitop_ID,Mesh_Vsitop,Vs30_Source,Wills_Vs30,Z2_5,Z1_0,Minimum_Vs,Vref_eff_ID,Vref_eff,Target_Vs30,SGT_Source_Filter_Frequency from CyberShake_Runs where Run_ID=%d" % old_id
cur.execute(query)
r = cur.fetchone()
        
query = "insert into CyberShake_Runs (Status,Comment,Site_ID,ERF_ID,SGT_Variation_ID,Rup_Var_Scenario_ID,SGT_Host,PP_Host,SGT_Time,PP_Time,Status_Time,Last_User,Notify_User,Velocity_Model_ID,Max_Frequency,Low_Frequency_Cutoff,Model_Vs30,Mesh_Vsitop_ID,Mesh_Vsitop,Vs30_Source,Wills_Vs30,Z2_5,Z1_0,Minimum_Vs,Vref_eff_ID,Vref_eff,Target_Vs30,SGT_Source_Filter_Frequency) values ('SGT Generated','Cloned from run %d'" % old_id
for f in r:
    if f is not None:
        if isinstance(f, str) or isinstance(f,datetime.datetime):
            query = '%s,"%s"' % (query, f)
        else:
            query = "%s,%s" % (query, str(f))
    else:
        query = "%s,NULL" % query
query = "%s)" % query
#print(query)
cur.execute(query)
conn.commit()
#Retrieve this run id and print it
query = "select Run_ID from CyberShake_Runs where Comment='Cloned from run %d' order by Run_ID desc limit 1" % old_id
cur.execute(query)
r = cur.fetchone()
print("%d" % int(r[0]))
cur.close()
conn.close()
