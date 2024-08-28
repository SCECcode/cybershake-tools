#!/usr/bin/env python3

import sys
import os
import optparse

sys.path.append("/home1/scottcal/.local/lib/python3.9/site-packages")

import pymysql
import fcntl

PENDING_PATH = "/home/shock/scottcal/runs/Study_24.8_management/pending.txt"

usage = "Usage: %prog -r <run_id> -s <next stage, LF or BB>"
parser = optparse.OptionParser(usage = usage)
parser.add_option("-r", "--run-id", dest="run_id", action="store", type="int", default=None, help="Run ID")
parser.add_option("-s", "--stage", dest="stage", action="store", type="str", default="LF", help="Next stage to run, either 'LF' or 'BB'")

(options, args) = parser.parse_args()
        
run_id = options.run_id
stage = options.stage

conn = pymysql.connect(user="cybershk_ro", passwd="CyberShake2007", host="moment", db="CyberShake")
cur = conn.cursor()

query = "select S.CS_Short_Name, R.ERF_ID, R.Rup_Var_Scenario_ID, R.SGT_Variation_ID, R.Velocity_Model_ID, R.Low_Frequency_Cutoff, R.Max_Frequency, R.SGT_Source_Filter_Frequency from CyberShake_Sites S, CyberShake_Runs R where R.Run_ID=%d and S.CS_Site_ID=R.Site_ID" % (run_id)

cur.execute(query)

r = cur.fetchall()
if len(r)>1:
	print("Error: more than 1 run matched the query '%s', aborting." % (query))
	sys.exit(2)

res = r[0]

name = res[0]
erf_id = int(res[1])
rup_var_scenario_id = int(res[2])
sgt_var_id = int(res[3])
vel_id = int(res[4])
lf_cutoff = float(res[5])
if res[6]==None:
	max_freq = lf_cutoff
else:
	max_freq = float(res[6])
src_freq = float(res[7])

#Lock the file while writing to it, so we don't conflict with AutoSubmit
#We are going to prepend it, since we want it to be sure to run once available

fp_out = open(PENDING_PATH, "r+")
#Begin lock
try:
	#Will block until lock is acquired
	fcntl.lockf(fp_out, fcntl.LOCK_EX)
except IOError:
	#Shouldn't get here, since we're blocking on lock
	print("Error acquiring lock on file %s, aborting." % PENDING_PATH, file=sys.stderr)
	sys.exit(1)

#Read current contents
data = fp_out.readlines()
#Reset to start
fp_out.seek(0, os.SEEK_SET)

#We were using a stack; sometimes this caused race conditions when PP workflows were planned before the stage out and registration jobs finished (since those run after Handoff).
#Instead, switch to a queue and write new PP jobs after old ones.

index = 0
while index<len(data) and data[index].split()[0]=="PP":
	fp_out.write(data[index])
	index += 1

if stage=="LF":
    fp_out.write("PP %s %d %d %d %d %f %f" % (name, erf_id, rup_var_scenario_id, sgt_var_id, vel_id, max_freq, src_freq))
    if lf_cutoff!=max_freq:
	    fp_out.write(" -hf")
    fp_out.write("\n")
elif stage=="BB":
    #Write out the other Stoch entries first
    while index<len(data) and data[index].split()[0]=="Stoch":
        fp_out.write(data[index])
        index += 1
    fp_out.write("Stoch %s %d %d %d %d %f %f" % (name, erf_id, rup_var_scenario_id, sgt_var_id, vel_id, max_freq, src_freq))
#Write rest of data
for i in range(index, len(data)):
	fp_out.write(data[i])
fp_out.flush()
#Free lock
fcntl.lockf(fp_out, fcntl.LOCK_UN)
fp_out.close()

sys.exit(0)
