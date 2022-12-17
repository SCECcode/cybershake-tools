#!/usr/bin/env python3

'''
10/29/13 - Modified to use the Pegasus RC client instead of Globus RLS
'''

# Imports
import os
import sys
import subprocess


# Constants
# These might get picked up from ~/.pegasusrc, but put them here just in case
#RC_HOST = "localhost:3306"
#RC_DB = "replica_catalog"
#RC_USER = "globus"
#RC_PASS = "GoTrojans!"
RC_HOST = "localhost:3306"
RC_DB = "/home/shock-ssd/scottcal/workflow/RC.sqlite"

# Globals


class RC_Entry:

    def __init__(self, lfn, pfn, pool=None):
        self.lfn = lfn
        self.pfn = pfn
        self.pool = pool


class RC:
    rc_path = None
    properties = None

    def __init__(self, rc_host=RC_HOST, rc_db=RC_DB):
        #self.rc_path = "jdbc:mysql://%s/%s" % (rc_host, rc_db)
        #self.properties = "-Dpegasus.catalog.replica=JDBCRC -Dpegasus.catalog.replica.db.driver=MySQL -Dpegasus.catalog.replica.db.url=%s -Dpegasus.catalog.replica.db.user=%s -Dpegasus.catalog.replica.db.password=%s" % (
        #    self.rc_path, RC_USER, RC_PASS)
        self.rc_path = "jdbc:sqlite:%s" % (rc_db)
        self.properties = "-Dpegasus.catalog.replica=JDBCRC -Dpegasus.catalog.replica.db.driver=sqlite -Dpegasus.catalog.replica.db.url=%s" % (self.rc_path)
        return

    def __runCommand(self, cmd):
        print("Running %s" % cmd, file=sys.stderr)
        try:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT)
            output = p.communicate()[0]
            retcode = p.returncode
            if retcode != 0:
                # print output
                # print "Non-zero exit code"
                return None
        except:
            # print sys.exc_info()
            # print "Failed to run cmd: " + str(cmd)
            return None

        output = output.splitlines()
        return output

    def getEntries(self, lfn):
        cmd = ['pegasus-rc-client']
        cmd.extend(self.properties.split())
        cmd.append('lookup')
        cmd.append(lfn)
        output = self.__runCommand(cmd)
        if (output == None):
            return None
        else:
            entry_list = []
            for l in output:
                tokens = l.split()
                pfn = tokens[1]
                if len(tokens) == 3:
                    pool = tokens[2].split('"')[1]
                    entry = RC_Entry(lfn, pfn, pool=pool)
                else:
                    entry = RC_Entry(lfn, pfn)
                entry_list.append(entry)
            return entry_list

    def addPool(self, pfn, pool):
        cmd = ['globus-rls-cli',  'attribute',  'add', pfn,
               'pool', 'pfn', 'string', pool, self.rls_host, ]
        output = self.__runCommand(cmd)
        if (output == None):
            return 1

        return 0

    def createLFN(self, lfn, pfn, pool=None):
        cmd = ['pegasus-rc-client']
        cmd.extend(self.properties.split())
        cmd.append('insert')
        cmd.append(lfn)
        cmd.append(pfn)
        if pool != None:
            cmd.append('pool="%s"' % pool)
        #cmd = ['globus-rls-cli', 'create', lfn, pfn, self.rls_host,]
        output = self.__runCommand(cmd)
        if (output == None):
            return 1

        return 0

    def delete(self, lfn, pfn):
        cmd = ['pegasus-rc-client']
        cmd.extend(self.properties.split())
        cmd.append('delete')
        cmd.append(lfn)
        cmd.append(pfn)
        #cmd = ['globus-rls-cli',  'delete',  lfn, pfn, self.rls_host,]
        output = self.__runCommand(cmd)
        if (output == None):
            return 1

        return 0
