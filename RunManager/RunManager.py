#!/usr/bin/env python

# Imports
import sys
from Database import *
from Config import *
from Run import *
from Site import *


# Constants
RUN_TABLE_NAME = "CyberShake_Runs"


class RunManager:
    database = None
    html = True
    valid = False

    def __init__(self, readonly=True):
        if (readonly == False):
            self.database = Database(DB_HOST, DB_PORT, DB_USER_WR, \
                                         DB_PASS_WR, DB_NAME)
        else:
            self.database = Database(DB_HOST, DB_PORT, DB_USER, DB_PASS, \
                                         DB_NAME)
        retval = self.database.open()
        if (retval == 0):
            self.valid = True
        else:
            self.valid = False
        self.database.useHTML(False)
        self.html = False

    def __del__(self):
        self.database.close()

    def _printError(self, str):
        if (self.html):
            print "%s<p>" % (str)
        else:
            print "%s" % (str)
        return

    def isValid(self):
        return self.valid

    def useHTML(self, flag):
        self.html = flag
        self.database.useHTML(flag)
        return

    def beginTransaction(self):
        self.database.beginTransaction()
        return

    def commitTransaction(self):
        self.database.commit()
        return
    
    def rollbackTransaction(self):
        self.database.rollback()
        return
    
    def __getSiteID(self, site):
        # Retrieve this site_id
        sqlcmd = "select CS_Site_ID from CyberShake_Sites s where s.CS_Short_Name ='%s'" % (site)
        if (self.database.execsql(sqlcmd) != 0):
            self._printError("Unable to execute query for site %s." % (site))
            return 0
        site_id = self.database.getResultsNext()
        if (site_id == None):
            self._printError("Site %s not found in DB." % (site))
            return 0
        
        return site_id[0]


    def __getSiteName(self, site_id):
        # Retrieve this site name
        sqlcmd = "select CS_Short_Name from CyberShake_Sites s where s.CS_Site_ID = %d" % (site_id)
        if (self.database.execsql(sqlcmd) != 0):
            self._printError("Unable to retrieve site_id %d" % (site_id))
            return None
    
        site = self.database.getResultsNext()
        if (site == None):
            self._printError("Site_id %d not found in DB." % (site_id))
            return None
        
        return site[0]


    def __getParamIDs(self, id_str):
        # Get list of IDs for the specified param
        if (id_str == "ERF"):
            sqlcmd = "select ERF_ID from ERF_IDs order by ERF_ID desc"
        elif (id_str == "SGT_VAR"):
            sqlcmd = "select SGT_Variation_ID from SGT_Variation_IDs order by SGT_Variation_ID desc"
        elif (id_str == "RUP_VAR"):
            sqlcmd = "select Rup_Var_Scenario_ID from Rupture_Variation_Scenario_IDs order by Rup_Var_Scenario_ID desc"
        else:
            self._printError("Unrecognized option %s" % (id_str))
            return None
        
        if (self.database.execsql(sqlcmd) != 0):
            self._printError("Unable to query for IDs.")
            return None
        id_list = self.database.getResultsAll()
        if (len(id_list) == 0):
            self._printError("No IDs found in DB for %s." % (id_str))
            return None
        return id_list


    
    def __getLatestID(self, id_str):
        # Get the most recent ID for the specified param
        id_list = self.__getParamIDs(id_str)
        if (id_list == None):
            return 0
        else:
            return id_list[0][0]



    def __isValidID(self, id_str, id):
        if (id_str == "ERF"):
            sqlcmd = "select count(*) from ERF_IDs where ERF_ID=%d" % (id)
        elif (id_str == "SGT_VAR"):
            sqlcmd = "select count(*) from SGT_Variation_IDs where SGT_Variation_ID=%d" % (id)
        elif (id_str == "RUP_VAR"):
            sqlcmd = "select count(*) from Rupture_Variation_Scenario_IDs where Rup_Var_Scenario_ID=%d" % (id)
        else:
            self._printError("Unrecognized option %s" % (id_str))
            return False
        
        if (self.database.execsql(sqlcmd) != 0):
            self._printError("Unable to query for IDs.")
            return False
        count = self.database.getResultsNext()
        if (count == None):
            self._printError("No IDs found in DB for %s-%d." % (id_str, id))
            return False
        if (count[0] == 0):
            self._printError("No IDs found in DB for %s-%d." % (id_str, id))
            return False
        else:
            return True


    def __getFieldDict(self, run, key=False):
        fields = {}

        if (key):
            if (run.getRunID() != None):
                fields["Run_ID"] = run.getRunID()
        if (run.getSiteID() != None):
            fields["Site_ID"] = run.getSiteID()
        if (run.getERFID() != None):
            fields["ERF_ID"] = run.getERFID()
        if (run.getSGTVarID() != None):
            fields["SGT_Variation_ID"] = run.getSGTVarID()
        if (run.getRupVarID() != None):
            fields["Rup_Var_Scenario_ID"] = run.getRupVarID()
        if (run.getStatus() != None):
            fields["Status"] = run.getStatus()
        if (run.getStatusTime() != None):
            fields["Status_Time"] = run.getStatusTime()
        if (run.getSGTHost() != None):
            fields["SGT_Host"] = run.getSGTHost()
        if (run.getSGTTime() != None):
            fields["SGT_Time"] = run.getSGTTime()
        if (run.getPPHost() != None):
            fields["PP_Host"] = run.getPPHost()
        if (run.getPPTime() != None):
            fields["PP_Time"] = run.getPPTime()
        if (run.getComment() != None):
            fields["Comment"] = run.getComment()
        if (run.getLastUser() != None):
            fields["Last_User"] = run.getLastUser()
        if (run.getJobID() != None):
            fields["Job_ID"] = run.getJobID()
        if (run.getSubmitDir() != None):
            fields["Submit_Dir"] = run.getSubmitDir()
            
        return fields


    def __getLastRunID(self):
        # Retrieve the last run_id produced from the auto-increment var
        sqlcmd = "select LAST_INSERT_ID()"
        if (self.database.execsql(sqlcmd) != 0):
            self._printError("Unable to query for last insert id.")
            return 0
        results = self.database.getResultsNext()
        if ((results == None) or (results[0] == 0)):
            self._printError("Unable to find last inserted run_id.")
            return 0
    
        return results[0]


    def __insertRun(self, run):
        # Construct column and data strings for SQL command
        fields = self.__getFieldDict(run)
        column_str = ""
        data_str = ""
        for f,v in fields.items():
            if (len(column_str) > 0):
                column_str = column_str + ', '
            column_str = column_str + str(f)
            if (len(data_str) > 0):
                data_str = data_str + ', '
            if (v == None):
                self._printError("Field pair %s=None, cannot insert record." % (f))
                return 0
            if (type(v) == str):
                data_str = data_str + "'" + str(v) + "'"
            else:
                data_str = data_str + str(v)

        # Execute SQL
        sqlcmd = "insert into %s (%s) values (%s)" % \
            (RUN_TABLE_NAME, column_str, data_str)
        if (self.database.execsql(sqlcmd) != 0):
            self._printError("Unable to add run for site %s." % \
                                 (run.getSiteName()))
            return 0

        # Get the run_id auto-increment value
        run_id = self.__getLastRunID()
        return run_id

        
    def __getRuns(self, run, lock):
        # Construct column and data strings for SQL command
        fields = self.__getFieldDict(run, key=True)
        where_str = ' and '
        for f,v in fields.items():
            if (where_str != ' and '):
                where_str = where_str + ' and '
            if (type(v) == str):
                where_str = where_str + "t.%s='%s'" % (f, str(v))
            else:
                where_str = where_str + "t.%s=%s"  % (f, str(v))

        if (lock == True):
            lock_str = "for update"
        else:
            lock_str = ""

        # Retrieve these runs and lock the rows for update
        sqlcmd = "select Run_ID, Site_ID, CS_Short_Name, ERF_ID, SGT_Variation_ID, Rup_Var_Scenario_ID, Status, Status_Time, SGT_Host, SGT_Time, PP_Host, PP_Time, Comment, Last_User, Job_ID, Submit_Dir from %s t, CyberShake_Sites s where s.CS_Site_ID = t.Site_ID %s order by t.Run_ID asc %s" % (RUN_TABLE_NAME, where_str, lock_str)
        if (self.database.execsql(sqlcmd) != 0):
            self._printError("Unable to retrieve run %s." % (run.getRunID()))
            return None
        else:
            run_data = self.database.getResultsAll()
            if ((len(run_data) == 0) or (run_data == None)):
                #self._printError("Matching runs not found in DB.")
                return None
            else:
                runs = []
                for r in run_data:
                    # Populate run object
                    newrun = Run()
                    newrun.setRunID(r[0])
                    newrun.setSiteID(r[1])
                    newrun.setSiteName(r[2])
                    newrun.setERFID(r[3])
                    newrun.setSGTVarID(r[4])
                    newrun.setRupVarID(r[5])
                    newrun.setStatus(r[6])
                    newrun.setStatusTime(r[7])
                    newrun.setSGTHost(r[8])
                    newrun.setSGTTime(r[9])
                    newrun.setPPHost(r[10])
                    newrun.setPPTime(r[11])
                    newrun.setComment(r[12])
                    newrun.setLastUser(r[13])
                    newrun.setJobID(r[14])
                    newrun.setSubmitDir(r[15])
                    runs.append(newrun)

                return runs


    def __getNewSites(self):
        # Get new sites
        deleted_runs = "select Site_ID from %s t where t.Site_ID = s.CS_Site_ID and not t.Status='%s'" % (RUN_TABLE_NAME, DELETED_STATE)
        complete_runs = "select Site_ID from %s r where r.Site_ID = s.CS_Site_ID and r.Status = '%s'" % (RUN_TABLE_NAME, DONE_STATE)
        sqlcmd = "select CS_Site_ID, CS_Short_Name, CS_Site_Lat, CS_Site_Lon, CS_Site_Name from CyberShake_Sites s where not exists (%s) and not exists (%s) order by s.CS_Site_ID" % (deleted_runs, complete_runs)
        if (self.database.execsql(sqlcmd) != 0):
            self._printError("Unable to retrieve sites")
            return None
        else:
            site_data = self.database.getResultsAll()
            if ((len(site_data) == 0) or (site_data == None)):
                #self._printError("Matching sites not found in DB.")
                return None
            else:
                sites = []
                for s in site_data:
                    # Populate site object
                    newsite = Site()
                    newsite.setSiteID(s[0])
                    newsite.setShortName(s[1])
                    newsite.setLatitude(s[2])
                    newsite.setLongitude(s[3])
                    newsite.setLongName(s[4])
                    sites.append(newsite)

                return sites


    def __updateRun(self, run):
        # Construct column and data strings for SQL command
        fields = self.__getFieldDict(run)
        assign_str = ""
        for f,v in fields.items():            
            if (len(assign_str) > 0):
                assign_str = assign_str + ', '
            if (type(v) == str):
                assign_str = assign_str + str(f) + "=" + "'" + str(v) + "'"
            else:
                assign_str = assign_str + str(f) + "=" + str(v)
                
        # Execute update
        sqlcmd = "update %s s set %s where s.Run_ID=%d" % \
            (RUN_TABLE_NAME, assign_str, run.getRunID())
        if (self.database.execsql(sqlcmd) != 0):
            self._printError("Unable to update run for run_id %d." % \
                                 (run.getRunID()))
            return 1

        return 0


    def __performComparison(self, oldrun, newrun):
        
        # Verify new state is valid state transition from old state
        if (not newrun.getStatus() in STATUS_STD[oldrun.getStatus()]):
            self._printError("Invalid new state %s, expecting one of %s." % \
                             (newrun.getStatus(), \
                                  str(STATUS_STD[oldrun.getStatus()])))
            return 1

        return 0


    def __performValidation(self, newrun):
        
        # Verify new state valid
        if (not newrun.getStatus() in STATUS_STD.keys()):
            self._printError("Invalid new state %s, expecting one of %s." % \
                             (newrun.getStatus(), str(STATUS_STD.keys())))
            return 1
        
        # Verify appropriate host has been specified
        if (newrun.getStatus() in SGT_STATES):
            if ((newrun.getSGTHost() == HOST_LIST[0]) or \
                    (newrun.getSGTHost() == None)):
                self._printError("Run is in a SGT state yet no SGT host specified.")
                return 1
            if (newrun.getSGTTime() == None):
                self._printError("Run is in a SGT state yet no SGT time specified.")
                return 1

        if (newrun.getStatus() in PP_STATES):
            if ((newrun.getPPHost() == HOST_LIST[0]) or \
                    (newrun.getPPHost() == None)):
                self._printError("Run is in a PP state yet no PP host specified.")
                return 1
            if (newrun.getPPTime() == None):
                self._printError("Run is in a PP state yet no PP time specified.")
                return 1

        return 0


    def __performCheckAndFill(self, run):
        # Get Site ID if needed
        if (run.getSiteID() == None):
            if (run.getSiteName() != None):
                site_id = self.__getSiteID(run.getSiteName())
                if (site_id == 0):
                    self._printError("Unable to find id of site %s" % \
                                         (run.getSiteName()))
                    return None
                run.setSiteID(site_id)
            else:
                self._printError("No site information provided in run!")
                return None

        # Set erf_id to the default if not specified
        if (run.getERFID() == None):
            id = self.__getLatestID("ERF")
            if (id == 0):
                return None
            else:
                run.setERFID(id)
        else:
            if (not self.__isValidID("ERF", run.getERFID())):
                return None
        
        # Set sgt_var_id to the default if not specified
        if (run.getSGTVarID() == None):
            id = self.__getLatestID("SGT_VAR")
            if (id == 0):
                return None
            else:
                run.setSGTVarID(id)
        else:
            if (not self.__isValidID("SGT_VAR", run.getSGTVarID())):
                return None
                
        # Set rup_var_id to the default if not specified
        if (run.getRupVarID() == None):
            id = self.__getLatestID("RUP_VAR")
            if (id == 0):
                return None
            else:
                run.setRupVarID(id)
        else:
            if (not self.__isValidID("RUP_VAR", run.getRupVarID())):
                return None
        
        if (run.getStatus() == None):
            run.setStatus(START_STATE)
        elif (not run.getStatus() in STATUS_STD.keys()):
            self._printError("State %s specified, was expecting one of %s" % \
                                 (run.getStatus(), str(STATUS_STD.keys())))
            return None
        run.setStatusTimeCurrent()
        
        if (run.getSGTHost() == None):
            run.setSGTHost(HOST_LIST[0])
        elif (not run.getSGTHost() in HOST_LIST):
            self._printError("SGT Host %s, was expecting one of %s" % \
                                 (run.getSGTHost(), HOST_LIST))
            return None
        run.setSGTTimeCurrent()

        if (run.getPPHost() == None):
            run.setPPHost(HOST_LIST[0])
        elif (not run.getPPHost() in HOST_LIST):
            self._printError("PP Host %s, was expecting one of %s" % \
                                 (run.getPPHost(), HOST_LIST))
            return None
        run.setPPTimeCurrent()

        if (run.getComment() == None):
            run.setComment("")

        if (run.getLastUser() == None):
            run.setLastUserCurrent()
        elif (not run.getLastUser() in USER_LIST):
            self._printError("User %s specified, was expecting one of %s" % \
                                 (run.getLastUser(), USER_LIST))
            return None
            
        if (run.getJobID() == None):
            run.setJobID("")

        if (run.getSubmitDir() == None):
            run.setSubmitDir("")
        
        return run


    def createRun(self, run):

        run.setRunID(None)

        run = self.__performCheckAndFill(run)
        if (run == None):
            return None

        # Insert run and save run_id
        run_id = self.__insertRun(run)
        # Result is the run_id, or 0 on error
        if (run_id == 0):
            return None
        else:
            pass
        
        run.setRunID(run_id)
        return run


    def createRunBySite(self, site):

        if (site == None):
            return None
        
        run = Run()
        run.setSiteName(site)
        return (self.createRun(run))
    

    def createRunByParam(self, site, erf_id, sgt_var_id, rup_var_id):
        if ((site == None) or (erf_id == None) or \
                (sgt_var_id == None) or (rup_var_id == None)):
            return None
        
        run = Run()
        run.setSiteName(site)
        run.setERFID(erf_id)
        run.setSGTVarID(sgt_var_id)
        run.setRupVarID(rup_var_id)
        return (self.createRun(run))


    def getRuns(self, run, lock=False):

        # Get Site ID if needed
        if (run.getSiteID() == None):
            if (run.getSiteName() != None):
                site_id = self.__getSiteID(run.getSiteName())
                if (site_id == 0):
                    self._printError("Unable to find id of site %s" % \
                                         (run.getSiteName()))
                    return None
                run.setSiteID(site_id)

        # Query existing data for this run query
        runs = self.__getRuns(run, lock)

        return runs


    def getRunByID(self, run_id, lock=False):

        if (run_id == None):
            self._printError("Requires a run_id to be specified.")
            return None

        run = Run()
        run.setRunID(run_id)

        # Query existing data for this run_id
        runs = self.getRuns(run, lock)
        if (runs == None):
            return None
        
        return runs[0]


    def getRunsByParam(self, site, erf_id, sgt_var_id, rup_var_id, lock=False):
        if ((site == None) or (erf_id == None) or \
                (sgt_var_id == None) or (rup_var_id == None)):
            self._printError("Requires site name, erf, sgt_var, and rup_var.")
            return None

        run = Run()
        run.setSiteName(site)
        run.setERFID(erf_id)
        run.setSGTVarID(sgt_var_id)
        run.setRupVarID(rup_var_id)
        
        # Query existing data for this combination
        runs = self.getRuns(run, lock)
        if (runs == None):
            return None
        
        return runs[0]


    def getSiteByID(self, site_id):
        return (self.__getSiteName(site_id))


    def getNewSites(self):
        return self.__getNewSites()


    def getParamIDs(self, param):
        return (self.__getParamIDs(param))


    def updateRun(self, run, orig_run = None):
        if (run.getRunID() == None):
            return 1

        # User wants field comparison on this run vs original run
        if (orig_run != None):

            # Run comparison checks
            if (self.__performComparison(orig_run, run) != 0):
                self._printError("Comparison of run and old_run failed.")
                return 1
            
        # Run validation checks
        if (self.__performValidation(run) != 0):
            self._printError("Validation of run failed.")
            return 1

        # Update timestamps
        run.setStatusTimeCurrent()
        if (run.getStatus() in SGT_STATES):
            run.setSGTTimeCurrent()
        elif (run.getStatus() in PP_STATES):
            run.setPPTimeCurrent()

        # Execute update
        retval = self.__updateRun(run)
        if (retval != 0):
            return 1
        else:
            pass

        return 0
    

    def deleteRunByID(self, run_id, last_user=None):
        if (run_id == None):
            self._printError("No run_id specified.")
            return 1
        run = Run()
        run.setRunID(run_id)
        run.setStatus(DELETED_STATE)
        run.setStatusTimeCurrent()
        if (last_user == None):
            run.setLastUserCurrent()
        else:
            if (not last_user in USER_LIST):
                self._printError("User_id %s, expecting one of %s." % \
                                     (last_user, str(USER_LIST)))
                return 1
            run.setLastUser(last_user)
        return self.updateRun(run)


