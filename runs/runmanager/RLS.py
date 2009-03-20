#!/usr/bin/env python


# Imports
import os
import sys
import subprocess

# Constants
RLS_HOST = "rls://shock.usc.edu"


# Globals


class RLS:

    def __init__(self):
        return

    def __runCommand(self, cmd):
        try:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            output = p.communicate()[0]
            retcode = p.returncode
            if retcode != 0:
                #print output
                #print "Non-zero exit code"
                return None
        except:
            #print sys.exc_info()
            #print "Failed to run cmd: " + str(cmd)
            return None

        output = output.splitlines()
        return output


    def getPFNs(self, lfn):

        cmd = ['globus-rls-cli',  'query',  'lrc', 'lfn',  lfn, RLS_HOST,]
        output = self.__runCommand(cmd)
        if (output == None):
            return None
        else:
            pfn_list = []
            for l in output:
                tokens = l.split(":", 1)
                if (len(tokens) == 2):
                    pfn = tokens[1]
                    pfn = pfn.strip()
                    pfn_list.append(pfn)
            return pfn_list


    def renamePFN(self, old_pfn, new_pfn):

        cmd = ['globus-rls-cli',  'rename',  'pfn', old_pfn, new_pfn, RLS_HOST,]
        output = self.__runCommand(cmd)
        if (output == None):
            return 1

        return 0


    def getPools(self, pfn):

        cmd = ['globus-rls-cli', 'attribute', 'query', pfn, 'pool', 'pfn', RLS_HOST,]
        output = self.__runCommand(cmd)
        if (output == None):
            return None
        else:
            pool_list = []
            for l in output:
                tokens = l.split(":", 2)
                if (len(tokens) == 3):
                    pool = tokens[2]
                    pool = pool.strip()
                    pool_list.append(pool)
            return pool_list


    def addPool(self, pfn, pool):
        cmd = ['globus-rls-cli',  'attribute',  'add', pfn, 'pool', 'pfn', 'string', pool, RLS_HOST,]
        output = self.__runCommand(cmd)
        if (output == None):
            return 1

        return 0


    def createLFN(self, lfn, pfn, pool=None):

        cmd = ['globus-rls-cli', 'create', lfn, pfn, RLS_HOST,]
        output = self.__runCommand(cmd)
        if (output == None):
            return 1

        if (pool != None):
            cmd = ['globus-rls-cli', 'attribute', 'add', pfn, 'pool', 'pfn', 'string', pool, RLS_HOST,]
            output = self.__runCommand(cmd)
            if (output == None):
                return 1

        return 0


    def delete(self, lfn, pfn):

        cmd = ['globus-rls-cli',  'delete',  lfn, pfn, RLS_HOST,]
        output = self.__runCommand(cmd)
        if (output == None):
            return 1

        return 0
