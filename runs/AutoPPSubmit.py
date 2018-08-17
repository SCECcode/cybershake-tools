#!/usr/bin/env python

# 1) Get pending list
# 2) See if any of those have finished
# 3) Figure out how many need to be submitted
# 4) Submit new wfs
# 5) Write modified pending list

import os
import re
import sys
import MySQLdb
import datetime
import subprocess
import fcntl

HOST = 'focal.usc.edu'
USER = 'cybershk_ro'
PASSWD = 'CyberShake2007'
DB = 'CyberShake'

SGT=0
PP=1
FULL=2

PENDING_FILE = "%s/pending.txt" % sys.path[0]
#We declare this here so we can lock on it
FP = open(PENDING_FILE, "r+")
LOG_FILE = "%s/auto_submit.log" % sys.path[0]
logFP = open(LOG_FILE, 'a')

MAX_SIMUL_SUBMIT = 3

#This represents a remote resource available for running workflows
class ExecutionSite:
	
	def __init__(self, name, max_wfs, full=False, SGT=False, PP=False):
		self.name = name
		self.max_wfs = max_wfs
		self.full = full
		self.SGT = SGT
		self.PP = PP
		self.SGT_args = []
		self.PP_args = []
		self.running = 0
		self.jobManagerList = []

	def setCustomSGTArgs(self, arg_list):
		self.SGT_args = arg_list

	def setCustomPPArgs(self, arg_list):
		self.PP_args = arg_list

	def setJobManagers(self, jm_list):
		self.jobManagerList = jm_list

	def getJobManagers(self):
		return self.jobManagerList

	def setX509Proxy(self, proxy):
		self.proxy = proxy

	def getX509Proxy(self):
		return self.proxy

	def setRunning(self, number):
		self.running = number

	def getRunning(self):
		return self.running


def getSiteFromName(executionSiteName):
	for s in execution_sites:
		if s.name==executionSiteName:
			return s
	return None

def incrementRunning(executionSiteName):
	for s in execution_sites:
		if s.name==executionSiteName:
			s.setRunning(s.getRunning()+1)
			return 0
	logFP.write("Couldn't find execution site to match %s\n" % executionSiteName)
	return 1

def initExecutionSites():
	sites = []
	#Set up execution sites
	#Blue Waters
	blue_waters_site = ExecutionSite("bluewaters", 15, full=True, SGT=True, PP=True)
	blue_waters_site.setX509Proxy("/tmp/x509up_u33527")
	bw_jobmanagers = ["h2ologin1.ncsa.illinois.edu:2119/jobmanager-pbs", "h2ologin1.ncsa.illinois.edu:2119/jobmanager-fork"]
	blue_waters_site.setJobManagers(bw_jobmanagers)
	sites.append(blue_waters_site)
	#Titan
	titan_site = ExecutionSite("titan", 15, full=False, SGT=True, PP=False)
	titan_site.setX509Proxy("/tmp/x509up_u7588")
	sgt_args = ["-d", "-sm"]
	titan_site.setCustomSGTArgs(sgt_args)
	sites.append(titan_site)
	return sites

def determineSite(workflow_phase):
	#Determine eligible sites
	eligibleSites = []
	if workflow_phase=="SGT":
		for s in execution_sites:
			if s.SGT==True:
				eligibleSites.append(s)
	elif workflow_phase=="PP":
		for s in execution_sites:
			if s.PP==True:
				eligibleSites.append(s)
	elif workflow_phase=="full":
		#Can split into pieces and do SGT and PP on separate machines
		eligibleSGTSites = []
		eligiblePPSites = []
		for s in execution_sites:
			if s.SGT==True:
				eligibleSGTSites.append(s)
			if s.PP==True:
				eligiblePPSites.append(s)
		if len(eligibleSGTSites)>1:
			site = eligibleSGTSites[0]
	                free_capacity = eligibleSGTSites[0].max_wfs - eligibleSGTSites[0].running
	                for s in eligibleSGTSites[1:]:
	                        if free_capacity<(s.max_wfs-s.running):
        	                        site = s
        	                        free_capacity = (s.max_wfs-s.running)
			eligibleSites.append(site)
		else:
			eligibleSites.append(eligibleSGTSites[0])
                if len(eligiblePPSites)>1:
                        site = eligiblePPSites[0]
                        free_capacity = eligiblePPSites[0].max_wfs - eligiblePPSites[0].running
                        for s in eligiblePPSites[1:]:
                                if free_capacity<(s.max_wfs-s.running):
                                        site = s
                                        free_capacity = (s.max_wfs-s.running)
                        eligibleSites.append(site)
                else:
                        eligibleSites.append(eligiblePPSites[0])
		return eligibleSites
	else:
		logFP.write("Could not find execution site to run %s workflows.\n" % workflow_phase)
		return None
	if len(eligibleSites)==1:
		#only 1 matching site, easy
		return eligibleSites[0]
	else:
		#Pick site with most free capacity; if tie, pick site which was specified first
		site = eligibleSites[0]
		free_capacity = eligibleSites[0].max_wfs - eligibleSites[0].running
		for s in eligibleSites[1:]:
			if free_capacity<(s.max_wfs-s.running):
				site = s
				free_capacity = (s.max_wfs-s.running)
		return site

def getCapacity():
	capacity = [0, 0, 0]
	for site in execution_sites:
		wfs_available = site.max_wfs - site.getRunning()
		if site.SGT:
			capacity[SGT] += wfs_available
		if site.PP:
			capacity[PP] += wfs_available
		if site.full:
			capacity[FULL] += wfs_available
	return capacity


def getPendingList():
	logFP.write("Getting pending list.\n")
	try:
		fcntl.lockf(FP, fcntl.LOCK_EX | fcntl.LOCK_NB)
	except IOError:
		logFP.write("AutoPPSubmit is aborting, since another instance is already running.\n")
		sys.exit(-1)
	data = FP.readlines()
	pendingList = []
	for line in data:
		pendingList.append(line.strip())
	return pendingList

def getRunsToSubmit(pendingList, cursor):
	sitesToSubmit = []
	#Check and see if condor_q is failing.  If so, skip this cycle.
	command = 'condor_q -dag | grep "Failed to fetch ads"'
	p = subprocess.Popen(command,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
	print command
	output = p.communicate()
	if output[0]!="" or output[1]!="": #something must have failed
#	if output!="":
		logFP.write("condor_q failed, skipping this cycle.\n")
		logFP.write("stdout: %s, stderr: %s\n" % (output[0], output[1]))
		return sitesToSubmit
	#See how many are running -- use condor_q instead of DB, since if jobs are submitted but there aren't any glideins, jobs won't show up in the DB
	command = 'condor_q -dag | grep -E "dagman|running" | grep cybershk | cut -d " " -f2'
	print command
	p = subprocess.Popen(command,shell=True,stdout=subprocess.PIPE)
	output = p.communicate()[0]
	#output now contains all the top-level dags from any workflow
	candidates = output.split("\n")
	numRunningJobs = 0
	#runningJobSites = []
	runningRunIDs = []
	for candidate in candidates:
		if candidate.strip()=="":
			continue
		#p = subprocess.Popen('condor_q -long %s | grep UserLog | grep _Integrated_' % candidate.strip(), shell=True, stdout=subprocess.PIPE)
		#Combine user log and execution sites to minimize condor_q calls
		cmd = 'condor_q -long %s | grep -E "UserLog|pegasus_execution_sites" ' % candidate.strip()
		p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
		output = p.communicate()[0]
		if output!="":
			#determine site
			#output looks like:
			#UserLog = "/home/scec-02/cybershk/runs/SMCA_SGT_dax/run_3864/dags/cybershk/pegasus/CyberShake_SGT_SMCA.dax/20150331T092702-0700/CyberShake_SGT_SMCA.dax-0.dag.dagman.log"
			#pegasus_execution_sites = "titan,shock"
			numRunningJobs += 1
			userLog = output.split("\n")[0]
			pieces = userLog.split("/")
			for piece in pieces:
				if re.match("run_\d+", piece):
					#runningJobSites.append(piece.split("_")[0])
					runningRunIDs.append(piece.split("_")[1])
			pegasusExecutionSites = output.split("\n")[1]
			remoteSite = pegasusExecutionSites.split('"')[1].split(",")[0]
			if remoteSite=="titan" and userLog.find("Integrated")!=-1:
				#Should check and see which stage it's in
				#If in AWP, give to titan; else give to bluewaters
				cmd = 'condor_q -dag | grep -E "subdax_AWP|PreCVM" | cut -d " " -f2'
				p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
				subdax = p.communicate()[0].split("\n")
				flag = False
				for s in subdax:
					cmd = 'condor_q -l %s | grep DAGManJobId' % (s.strip())
					print cmd
					p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
					line = p.communicate()[0]
					#Don't want .0 part of candidate
					if line.strip()==candidate.split(".")[0].strip():
						incrementRunning("titan")
						flag = True
						break
				if not flag:
					#This means we aren't the SGT phase, and the credit should go to bluewaters instead
					incrementRunning("bluewaters")
			else:
				#Update the execution site with this running job
				incrementRunning(remoteSite)
	for site in execution_sites:
		logFP.write("%d workflows(s) running on site %s.\n" % (site.getRunning(), site.name))
	logFP.write("%d total workflow(s) running:" % numRunningJobs)
	for s in runningRunIDs:
		logFP.write(" %s" % s)
	logFP.write("\n")
	#Next, check remaining capacity with available sites
	capacity = getCapacity()
	if sum(capacity)==0:
		#No space available anywhere to run more jobs
		return sitesToSubmit
	else:
		#Add more jobs
		index = 0
		#Need to check capacity each time, since submitting a site could impact all 3 types of workflows
		while sum(capacity)>0 and index<len(pendingList) and len(sitesToSubmit)<MAX_SIMUL_SUBMIT:
			pieces = pendingList[index].split()
                        if len(pieces)==0:
                                #It's a blank line, ignore
                                pendingList.pop(index)
                                continue
			flag = 0
			# Make sure it hasn't been submitted before and just not updated in the DB yet
                        for runningID in runningRunIDs:
                                #Do a DB query to see if all the run parameters match
                                run_query = "select S.CS_Short_Name, R.ERF_ID, R.Rup_Var_Scenario_ID, R.SGT_Variation_ID, R.Velocity_Model_ID, R.Low_Frequency_Cutoff, R.SGT_Source_Filter_Frequency from CyberShake_Runs R, CyberShake_Sites S where R.Site_ID=S.CS_Site_ID"
				cursor.execute(run_query)
                                db_run_data = cursor.fetchall()
                                #If all the values match, we're going to assume this run is already running and skip it
                                if db_run_data[0]==pieces[1] and db_run_data[1]==pieces[2] and db_run_data[2]==pieces[3] and db_run_data[3]==pieces[4] and db_run_data[4]==pieces[5] and db_run_data[5]==pieces[6] and db_run_data[6]==pieces[7]:
                                        flag = 1
                                        pendingList.pop(index)
                                        break
			if flag==1:
				continue
			if pieces[0]=="SGT":
				if capacity[SGT]==0:
					logFP.write("No SGT capacity available, skipping site %s.\n" % pieces[1])
					continue
				else:
					logFP.write("Preparing site %s to be executed.\n" % pieces[1])
					executionSite = determineSite("SGT")
					if executionSite==None:
						continue
					logFP.write("Selected execution site %s.\n" % executionSite.name)
					executionSite.running += 1
					sitesToSubmit.append("%s %s" % (executionSite.name, pendingList[index]))
					pendingList.pop(index)
			elif pieces[0]=="PP":
				if capacity[PP]==0:
					logFP.write("No PP capacity available, skipping site %s.\n" % pieces[1])
					continue
				#Take the most recent run, if there are multiple
				query = "select R.Status from CyberShake_Runs R, CyberShake_Sites S where S.CS_Site_ID=R.Site_ID and S.CS_Short_Name='%s' and R.ERF_ID=%s and R.Rup_Var_Scenario_ID=%s and R.SGT_Variation_ID=%s and R.Velocity_Model_ID=%s and R.Low_Frequency_Cutoff=%s and R.SGT_Source_Filter_Frequency=%s order by R.Run_ID desc" % (pieces[1], pieces[2], pieces[3], pieces[4], pieces[5], pieces[6], pieces[7])
				logFP.write("Executing query %s.\n" % query)
				cursor.execute(query)
				runs = cursor.fetchall()
	                        if len(runs)==0:
	                                logFP.write("No results found for site %s.\n" % pieces[1])
					continue
	                        if runs[0][0]=='SGT Generated':
         	                	logFP.write("Preparing site %s to be executed.\n" % (pieces[1]))
					executionSite = determineSite("PP")
					if executionSite==None:
						continue
					logFP.write("Selected execution site %s.\n" % executionSite.name)
					executionSite.running += 1
         	                	sitesToSubmit.append("%s %s" % (executionSite.name, pendingList[index]))
         	                	pendingList.pop(index)
	                        elif runs[0][0]=='PP Error':
	                                logFP.write("Site %s is in status PP Error, retrying.\n" % pieces[1])
                                        executionSite = determineSite("PP")
					if executionSite==None:
						continue
					logFP.write("Selected execution site %s.\n" % executionSite.name)
                                        executionSite.running += 1
	                                sitesToSubmit.append("%s %s" % (executionSite.name, pendingList[index]))
	                                pendingList.pop(index)
	                        else:
                                	#otherwise run is in some other state -- PP running, error, don't worry about it right now.
                                	logFP.write("Site %s is in state %s.\n" % (pieces[1], runs[0][0]))
                                	index += 1
			elif pieces[0]=="full":
				if capacity[FULL]==0:
                                        logFP.write("No full capacity available, skipping site %s.\n" % pieces[1])
                                        continue
                                else:
                                        logFP.write("Preparing site %s to be executed.\n" % pieces[1])
                                        executionSite = determineSite("full")
					if executionSite==None:
						continue
					#For full we select 2 sites, but they might be the same
					logFP.write("Selected execution sites %s,%s.\n" % (executionSite[0].name, executionSite[1].name))
                                        executionSite[0].running += 1
                                        sitesToSubmit.append("%s,%s %s" % (executionSite[0].name, executionSite[1].name, pendingList[index]))
                                        pendingList.pop(index)
			else:
				logFP.write("Workflow type %s isn't recognized, skipping.\n" % pieces[0])
				index +=1 
				continue
			capacity = getCapacity()
	return sitesToSubmit

	'''
	if numRunningJobs>=NUM_SIMUL_WFS: #we have at least NUM_SIMUL_WFS running, don't need to submit any more
		return sitesToSubmit
	# otherwise, need to add more jobs.
	else:
		prefix = "select R.Status from CyberShake_Runs R, CyberShake_Sites S where S.CS_Site_ID=R.Site_ID "
		needToFind = NUM_SIMUL_WFS-numRunningJobs
		#adjust needToFind so we don't submit more than MAX_SIMUL_SUBMIT
		needToFind = min(needToFind, MAX_SIMUL_SUBMIT)
		index = 0
		while len(sitesToSubmit)<needToFind and index<len(pendingList): # take the most recent, if there are multiple
                        pieces = pendingList[index].split()
                        if len(pieces)==0:
                                #It's a blank line, ignore
                                pendingList.pop(index)
                                continue

			flag = 0
                        # Make sure it hasn't been submitted before and just not updated in the DB yet
			for runningID in runningRunIDs:
				#Do a DB query to see if all the run parameters match
				run_query = "select S.CS_Short_Name, R.ERF_ID, R.Rup_Var_Scenario_ID, R.SGT_Variation_ID, R.Velocity_Model_ID from CyberShake_Runs R, CyberShake_Sites S where R.Site_ID=S.CS_Site_ID"
				cursor.execute(run_query)
				db_run_data = cursor.fetchall()
				#If all the values match, we're going to assume this run is already running and skip it
				if db_run_data[0]==pieces[0] and db_run_data[1]==pieces[1] and db_run_data[2]==pieces[2] and db_run_data[3]==pieces[3] and db_run_data[4]==pieces[4]:
					#if pieces[0]==runningSite:
					flag = 1
					pendingList.pop(index)
					break
			if flag==1:
				continue
			#Do not need to do this query - we're running the whole workflow, so we don't need to find a viable SGT run to start from
			
			# take the most recent, if there are multiple
			query = "%s and R.ERF_ID=%s and R.Rup_Var_Scenario_ID=%s and R.SGT_Variation_ID=%s and R.Velocity_Model_ID=%s " % (prefix, pieces[1], pieces[2], pieces[3], pieces[4])
			#don't need to search for HF because that's only a post-processing thing
			query = "%s and S.CS_Short_Name='%s' order by R.Run_ID desc" % (query, pieces[0])
			logFP.write("Executing query %s." % query)
			cursor.execute(query)
			runs = cursor.fetchall()
			if len(runs)==0:
				logFP.write("No results found for site %s.\n" % pieces[0])
				return sitesToSubmit
			if runs[0][0]=='SGT Generated':
				logFP.write("Preparing site %s to be executed.\n" % (pieces[0]))
				sitesToSubmit.append(pendingList[index])
				pendingList.pop(index)
			elif runs[0][0]=='PP Error':
				logFP.write("Site %s is in status PP Error, retrying.\n" % pieces[0])
				sitesToSubmit.append(pendingList[index])
				pendingList.pop(index)
			elif runs[0][0]=='Verified':
				#Need to create new run
				#logFP.write("Site %s finished, removing from pending list.\n" % pieces[0])
				logFP.write("Creating new PP run for site %s.\n" % pieces[0])
				sitesToSubmit.append(pendingList[index])
				pendingList.pop(index)
			else:
				#otherwise run is in some other state -- PP running, error, don't worry about it right now.
				logFP.write("Site %s is in state %s.\n" % (pieces[0], runs[0][0]))
				index += 1
			
			logFP.write("Preparing '%s' to be executed.\n" % (pendingList[index]))
			sitesToSubmit.append(pendingList[index])
			pendingList.pop(index)
		#now we have needToFind number of sites
		return sitesToSubmit
	'''

#pending file consists of:
#<SGT|PP|full> <site> <erf_id> <rv_id> <sgt_id> <vel_id> <frequency> <source frequency> [-hf] [r]


def submitRuns(runsToSubmit):
	prefix = 'cd /home/scec-02/cybershk/runs'
	for run in runsToSubmit:
		#entry looks like:
		#<executionSite> <SGT|PP|full> <site> <erf_id> <rv_id> <sgt_id> <vel_id> <frequency> <source frequency> [-hf] [-r]

		global_pp_args = "-ds -nb -r"
		pieces = run.split()
		logFP.write("Submitting site %s.\n" % pieces[2])
	 	#executeString = "%s;source %s/setup.sh;./create_pp_wf.sh %s %d %d %d -p 80 2>&1 >> %s/%s.log;./plan_pp.sh %s ranger 2>&1 >> %s/%s.log;./run_pp.sh -n cybershk@gmail.com %s 2>&1 >> %s/%s.log" % (prefix, sys.path[0], run, ERF_ID, RUP_VAR_ID, SGT_VAR_ID, sys.path[0], run, run, sys.path[0], run, run, sys.path[0], run)
		restart = False
		execution_site_name = pieces[0]
		execution_site = []
		if execution_site_name.find(",")!=-1:
			exec_sites = execution_site_name.split(",")
			execution_site.append(getSiteFromName(exec_sites[0]))
			execution_site.append(getSiteFromName(exec_sites[1]))
		else:
			execution_site.append(getSiteFromName(execution_site_name))
		if execution_site==None or execution_site[0]==None:
			logFP.write("Error finding site object for execution site name %s, skipping this workflow.\n" % execution_site_name)
			continue
		workflow_type = pieces[1]
		site = pieces[2]
		erf_id = int(pieces[3])
		rv_id = int(pieces[4])
		sgt_id = int(pieces[5])
		vel_model = "-v4"
		vel_id = int(pieces[6])
                partitions = 8
		if vel_id==4: 
                        #CVM-H
			vel_model = "vh"
		elif vel_id==5:
			#CVM-S4.26
			vel_model = "vsi"
		elif vel_id==7:
			#CVM-H, no GTL
			vel_model = "vhng"
		elif vel_id==8:
			#BBP 1D
			vel_model = "vbbp"
		freq = float(pieces[7])
		src_freq = float(pieces[8])
		hf_string = ""
		if len(pieces)>9:
			#Check for hf_string or restart
			if pieces[9]=='r':
				restart = True
			else:
				hf_string = "-hf %s" % pieces[9]
				partitions = 12

		sourceString = "%s;source %s/setup.sh" % (prefix, sys.path[0])
		if not restart:
			#Pick a site
			if workflow_type=="SGT":
				createString = "./create_sgt_dax.sh -s %s -v %s -e %d -r %d -g %d -f %f -q %f" % (site, vel_model, erf_id, rv_id, sgt_id, freq, src_freq)
				sgtArgs = execution_site[0].SGT_args
				if len(sgtArgs)>0:
					createString = "%s --sgtargs %s" % (createString, " ".join(sgtArgs))
				executeString = "%s; %s" % (sourceString, createString)
				print >> sys.stderr, executeString
				rc = os.system(executeString)
	                        if rc!=0:
	                                print >> sys.stderr, "Error executing %s, aborting." % executeString
	                                return 1
				run_table_file = "/home/scec-02/cybershk/runs/%s_SGT_dax/run_table.txt" % (site)
			elif workflow_type=="PP":
				createString = "./create_pp_wf.sh -s %s -v %s -e %d -r %d -g %d -f %f -q %f" % (site, vel_model, erf_id, rv_id, sgt_id, freq, src_freq)
				#ppArgs = "--ppargs %s -p %d %s" % (global_pp_args, partitions, " ".join(execution_site.PP_args))
				ppArgs = "--ppargs %s %s" % (global_pp_args, " ".join(execution_site[0].PP_args))
				createString = "%s %s" % (createString, ppArgs)
				executeString = "%s; %s" % (sourceString, createString)
				print >> sys.stderr, executeString
				rc = os.system(executeString)
                                if rc!=0:
                                        print >> sys.stderr, "Error executing %s, aborting." % executeString
                                        return 1
				run_table_file = "/home/scec-02/cybershk/runs/%s_PP_dax/run_table.txt" % (site)
			elif workflow_type=="full":
				createString = "./create_full_wf.sh -s %s -v %s -e %d -r %d -g %d -f %f -q %f" % (site, vel_model, erf_id, rv_id, sgt_id, freq, src_freq)
				sgtArgs = "--sgtargs -ss %s" % (execution_site[0].name)
				if len(execution_site[0].SGT_args):
                                	sgtArgs = "%s %s" % (sgtArgs, " ".join(execution_site[0].SGT_args))
				createString = "%s %s" % (createString, sgtArgs)
				#ppArgs = "--ppargs %s -p %d %s" % (global_pp_args, partitions, " ".join(execution_site.PP_args))
				ppArgs = "--ppargs -ps %s %s %s" % (execution_site[1].name, global_pp_args, " ".join(execution_site[1].PP_args))
				createString = "%s %s" % (createString, ppArgs)
				executeString = "%s; %s" % (sourceString, createString)
				print >> sys.stderr, executeString
                                rc = os.system(executeString)
                                if rc!=0:
                                        print >> sys.stderr, "Error executing %s, aborting." % executeString
                                        return 1
				run_table_file = "/home/scec-02/cybershk/runs/%s_Integrated_dax/run_table.txt" % (site)
			else:
				logFP.write("Don't recognize workflow type %s, skipping." % workflow_type)
				continue
		else:
			if workflow_type=="SGT":
				run_table_file = "/home/scec-02/cybershk/runs/%s_SGT_dax/run_table.txt" % (site)
			elif workflow_type=="PP":
				run_table_file = "/home/scec-02/cybershk/runs/%s_PP_dax/run_table.txt" % (site)
			elif workflow_type=="full":
				run_table_file = "/home/scec-02/cybershk/runs/%s_Integrated_dax/run_table.txt" % (site)
		fp_in = open(run_table_file, "r")
                data = fp_in.readlines()
                fp_in.close()
		run_id = -1
		#Search in reverse order, since want most recent
                for line in data[::-1]:
                	table_pieces = line.split()
                        if len(table_pieces)<7:
                	        #Old-format, ignore it
                                continue
                        if int(table_pieces[2])==erf_id and int(table_pieces[3])==sgt_id and int(table_pieces[4])==rv_id and int(table_pieces[5])==vel_id and float(table_pieces[6])==freq and float(table_pieces[7])==src_freq:
				run_id = int(table_pieces[0])
				break
		if run_id==-1:
			#Couldn't find a matching run in the runtable.
			sys.stderr.write("Couldn't find an entry in run_table.txt for site %s with erf_id %d, rv_id %d, sgt_id %d, vel_id %d, freq %f, src_freq %f.  Suggests a bug in AutoPPSubmit." % (site, erf_id, rv_id, sgt_id, vel_id, freq, src_freq))
			continue
		#Plan
		executeString = "%s" % sourceString
		if not restart:
	                if workflow_type=="SGT":
	                        planString = "./plan_sgt.sh %s %d %s" % (site, run_id, execution_site_name)
			elif workflow_type=="PP":
				planString = "./plan_pp.sh %s %d %s scottcal scottcal@usc.edu" % (site, run_id, execution_site_name)
			elif workflow_type=="full":
				#We want to use the site name from the SGT part
				planString = "./plan_full.sh %s %d %s" % (site, run_id, execution_sites[0].name)
			else:
				logFP.write("Don't recognize workflow type %s, skipping." % workflow_type)
	                        continue
			executeString = "%s; %s" % (executeString, planString)
		#Run
		restartString = ""
		if restart:
			restartString = "-r"
		if workflow_type=="SGT":
			runString = "./run_sgt.sh %s %s %d" % (restartString, site, run_id)
		elif workflow_type=="PP":
			runString = "./run_pp.sh %s %s %d" % (restartString, site, run_id)		
		elif workflow_type=="full":
			runString = "./run_full.sh %s %s %d" % (restartString, site, run_id)
		else:
                        logFP.write("Don't recognize workflow type %s, skipping." % workflow_type)
                        continue
		executeString = "%s; %s" % (executeString, runString)
                print >> sys.stderr, executeString
                rc = os.system(executeString)
                return rc

		'''
		if not restart:
			#createString = "./create_full_wf.sh %s %d %d %d %s %s" % (vel_model, erf_id, rv_id, sgt_id, site, opt_args)
			createString = "./create_pp_wf.sh %s %s %d %d %d -p %d %s %s" % (site, vel_model, erf_id, rv_id, sgt_id, partitions, hf_string, opt_args)
			#Run create, so run_table.txt gets populated
			executeString = "%s; %s" % (sourceString, createString)
			print >> sys.stderr, executeString
			rc = os.system(executeString)
			if rc!=0:
				print >> sys.stderr, "Error executing %s, aborting." % executeString
				return 1
		#Determine run ID by looking at run_table.txt
		#fp_in = open("/home/scec-02/cybershk/runs/%s_Integrated_dax/run_table.txt" % (site), "r")
		fp_in = open("/home/scec-02/cybershk/runs/%s_PP_dax/run_table.txt" % (site), "r")
		data = fp_in.readlines()
		fp_in.close()
		run_id = -1
		for line in data:
			table_pieces = line.split()
			if len(table_pieces)<6:
				#We found an old-format entry, ignore it.
				continue
			if int(table_pieces[2])==erf_id and int(table_pieces[3])==sgt_id and int(table_pieces[4])==rv_id and int(table_pieces[5])==vel_id:
				run_id = int(table_pieces[0])
				#Don't break; want to keep last entry in the run_table which meets all the criteria
		if run_id==-1:
			#Couldn't find a run table entry which corresponds.  This means we've got a bug, somewhere.
			sys.stderr.write("Couldn't find an entry in run_table.txt for site %s with erf_id %d, rv_id %d, sgt_id %d, and vel_id %d.  Suggests a bug in AutoPPSubmit." % (site, erf_id, rv_id, sgt_id, vel_id))
			continue
		execution_system = "bluewaters"
		if not restart:
			#planString = "./plan_full.sh %s %d %s" % (site, run_id, execution_system)
			planString = "./plan_pp.sh %s %d %s scottcal scottcal@usc.edu" % (site, run_id, execution_system)
			#runString = "./run_full.sh -n cybershk@gmail.com %s %d" % (site, run_id)
			runString = "./run_pp.sh -n cybershk@gmail.com %s %d" % (site, run_id)
			executeString = "%s; %s; %s" % (sourceString, planString, runString)
		else:
			#runString = "./run_full.sh -r -n cybershk@gmail.com %s %d" % (site, run_id)
			runString = "./run_pp.sh -r -n cybershk@gmail.com %s %d" % (site, run_id)
			executeString = "%s; %s" % (sourceString, runString)
		#executeString = "%s;source %s/setup.sh;./create_pp_wf.sh %s %s %d %d %d -p %d %s %s;./plan_pp.sh %s %d stampede scottcal scottcal@usc.edu;./run_pp.sh -n cybershk@gmail.com %s" % (prefix, sys.path[0], pieces[0], vel_model, int(pieces[1]), int(pieces[2]), int(pieces[3]), partitions, hf_string, opt_args, pieces[0], pieces[0])
		print >> sys.stderr, executeString
		rc = os.system(executeString)
		return rc
		'''

def writePendingFile(pendingList):
	FP.seek(os.SEEK_SET)
	for site in pendingList:
		FP.write("%s\n" % site)
	FP.flush()
	FP.truncate()
	fcntl.lockf(FP, fcntl.LOCK_UN)
	FP.close()

#Init sites
execution_sites = initExecutionSites()

#First, make sure the gridftp jobmanagers are up
for s in execution_sites:
	for jm in s.getJobManagers():
		exitcode = os.system("export X509_USER_PROXY=%s; /usr/bin/globusrun -a -r %s" % (s.getX509Proxy(), jm))
		if exitcode!=0:
			logFP.write("Error %d with jobmanager %s.\n" % (exitcode, jm))
			sys.exit(1)

now = datetime.datetime.now().isoformat()
logFP.write("%s\n" % now)
print now
pendingList = getPendingList()
if len(pendingList)==0:
	logFP.write("No more pending jobs.\n")
	sys.exit(0)
connection = MySQLdb.connect(host=HOST, user=USER, passwd=PASSWD, db=DB)
listToSubmit = getRunsToSubmit(pendingList, connection.cursor())
rc=0
if len(listToSubmit)>0:
	rc = submitRuns(listToSubmit)
if rc!=0:
	#Don't want to put sitename back in the list
	for i in range(0, len(listToSubmit)):
		listToSubmit[i] = " ".join(listToSubmit[i].split(" ")[1:])
	listToSubmit.extend(pendingList)
	writePendingFile(listToSubmit)
else:
	writePendingFile(pendingList)
connection.close()
logFP.flush()
logFP.close()
