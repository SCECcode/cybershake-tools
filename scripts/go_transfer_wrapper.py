#!/usr/bin/env python3

'''This script is invoked instead of pegasus-transfer.  If the transfer is to a CARC endpoint (hpc-transfer*.usc.edu), then use GO; otherwise, pass thru to pegasus-transfer.

[
 { "type": "transfer",
   "linkage": "output",
   "lfn": "Duration_USC_7239_2453_5.dur",
   "id": 1,
   "src_urls": [
     { "site_label": "summit", "url": "gsiftp://gridftp.ccs.ornl.gov/gpfs/alpine/scra
tch/callag/geo112/execdir/scottcal/pegasus/CyberShake_Integrated_USC/20211206T093608-
0800/././CyberShake_USC_Synth_USC_dax_0/./Duration_USC_7239_2453_5.dur" }
   ],
   "dest_urls": [
     { "site_label": "shock", "url": "gsiftp://hpc-transfer1.usc.edu/project/scec_608
/cybershake/results/PPFiles/USC/7239/Duration_USC_2453_5.dur" }
   ] }
 ,
 { "type": ...


  ] }
]
'''

import sys
import os
import datetime
import subprocess

PEGASUS_TRANSFER = "/home/shock-ssd/scottcal/pegasus/default/bin/pegasus-transfer"

go_endpoint_dict = dict()
go_endpoint_dict['hpc-transfer1.usc.edu'] = '56569ec1-af41-4745-a8d1-8514231c7a6d'
go_endpoint_dict['hpc-transfer2.usc.edu'] = '56569ec1-af41-4745-a8d1-8514231c7a6d'
go_endpoint_dict['gridftp.ccs.ornl.gov'] = 'ef1a9560-7ca1-11e5-992c-22000b96db58'

class FilePair:

    def __init__(self):
        self.type = ""
        self.linkage = ""
        self.lfn = ""
        self.id = -1
        self.src_site = ""
        self.src_url = ""
        self.src_protocol = ""
        self.src_host = ""
        self.src_path = ""
        self.dest_site = ""
        self.dest_url = ""
        self.dst_protocol = ""
        self.dst_host = ""
        self.dst_path = ""

    def printFields(self):
        print("type=%s" % self.type)
        print("linkage=%s" % self.linkage)
        print("lfn=%s" % self.lfn)
        print("id=%d" % self.id)
        print("src_site=%s" % self.src_site)
        print("src_url=%s" % self.src_url)
        print("src_protocol=%s" % self.src_protocol)
        print("src_host=%s" % self.src_host)
        print("src_path=%s" % self.src_path)
        print("dst_site=%s" % self.dst_site)
        print("dst_url=%s" % self.dst_url)
        print("dst_protocol=%s" % self.dst_protocol)
        print("dst_host=%s" % self.dst_host)
        print("dst_path=%s" % self.dst_path)
        

    def parseFilePair(self, data):
        pieces = data[0].split(":")
        self.type = pieces[1][:-2]
        pieces = data[1].split(":")
        self.linkage = pieces[1][:-2]
        pieces = data[2].split(":")
        self.lfn = pieces[1][:-2]
        pieces = data[3].split(":")
        self.id = int(pieces[1][:-2])
        pieces = data[5].split(",")
        subpieces = pieces[0].split(":")
        self.src_site = subpieces[1].strip()
        subpieces = pieces[1].split(":")
        self.src_url = ":".join(subpieces[1:]).strip()
        while self.src_url[-1]!='"':
            self.src_url = self.src_url[:-1]
        pieces = data[8].split(",")
        subpieces = pieces[0].split(":")
        self.dst_site = subpieces[1].strip()
        subpieces = pieces[1].split(":")
        self.dst_url = ":".join(subpieces[1:]).strip()[:-2]

        pieces = self.src_url.split("://")
        self.src_protocol = pieces[0].strip()[1:]
        subpieces = pieces[1].split("/")
        self.src_host = subpieces[0]
        self.src_path = "/%s" % "/".join(subpieces[1:])[:-1]

        pieces = self.dst_url.split("://")
        self.dst_protocol = pieces[0].strip()[1:]
        subpieces = pieces[1].split("/")
        self.dst_host = subpieces[0]
        self.dst_path = "/%s" % "/".join(subpieces[1:])[:-1]


    def writeToFile(self, fp_out):
        fp_out.write(' { "type": %s,\n' % self.type)
        fp_out.write('   "linkage": %s,\n' % self.linkage)
        fp_out.write('   "lfn": %s,\n' % self.lfn)
        fp_out.write('   "id": %d,\n' % self.id)
        fp_out.write('   "src_urls": [\n')
        fp_out.write('     { "site_label": %s, "url": %s }\n' % (self.src_site, self.src_url))
        fp_out.write('   ],\n')
        fp_out.write('   "dest_urls": [\n')
        fp_out.write('     { "site_label": %s, "url": %s }\n' % (self.dst_site, self.dst_url))
        fp_out.write('   ] }\n')

def goTransferSubmit(go_src_host, go_dst_host, go_filename):
    go_src_endpoint = go_endpoint_dict[go_src_host]
    go_dst_endpoint = go_endpoint_dict[go_dst_host]
    cmd = "/home1/scottcal/.local/bin/globus transfer --batch %s %s %s" % (go_filename, go_src_endpoint, go_dst_endpoint)
    cp = subprocess.run(cmd.split(),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    output_string = cp.stdout.decode('utf-8')
    print(output_string)
    task_id = output_string.split(":")[2]
    return task_id

def waitTransfer(task_id):
    cmd = "/home1/scottcal/.local/bin/globus task wait --polling-interval 15 %s" % task_id
    print(cmd)
    os.system(cmd)

def goTransfer(go_src_host, go_dst_host, go_filename):
    task_id = goTransferSubmit(go_src_host, go_dst_host, go_filename)
    waitTransfer(task_id)

filepairs = []

input_data = sys.stdin.readlines()

i = 0
while i<len(input_data):
    line = input_data[i]
    if line[0]=='[':
        #Starts file
        i+=1
        continue
    elif line[1]=='{':
        #Starts FilePair
        fp = FilePair()
        fp.parseFilePair(input_data[i:i+10])
        filepairs.append(fp)
        #fp.printFields()
        i += 10
        continue
    elif line[1]==',':
        #Between filepairs
        i += 1
        continue
    elif line[0]==']':
        #Ends file
        break
    else:
        print("Don't know what to do with line %s, aborting." % line)
        sys.exit(2)


current_dt = datetime.datetime.now().timestamp()
millis = int(current_dt*1000.0)
tmp_filenames = []
pt_tmp_filename = "/tmp/pegasus-transfer-%d" % millis
pt_out = open(pt_tmp_filename, "w")
tmp_filenames.append(pt_tmp_filename)
go_tmp_filename = "/tmp/go-%d" % millis
go_out = open(go_tmp_filename, "w")
tmp_filenames.append(go_tmp_filename)
first = True
go_transfer = False
go_from_host = None
go_to_host = None
go_file_length = 0
MAX_GO_FILE_LENGTH = 100000
task_ids = []

for fp in filepairs:
    if fp.dst_url.find("hpc-transfer1.usc.edu")==-1 and fp.dst_url.find("hpc-transfer2.usc.edu")==-1:
        #let pegasus-transfer handle it
        if first is True:
            pt_out.write("[\n")
            first = False
        else:
            pt_out.write(" ,\n")
        fp.writeToFile(pt_out)
    else:
        if go_transfer is False:
            go_transfer = True
            go_from_host = fp.src_host
            go_to_host = fp.dst_host
        if go_file_length>=MAX_GO_FILE_LENGTH:
            #Transfer this file, create a new one
            go_out.flush()
            go_out.close()
            task_ids.append(goTransferSubmit(go_from_host, go_to_host, go_tmp_filename))
            go_file_length = 0
            current_dt = datetime.datetime.now().timestamp()
            millis = int(current_dt*1000.0)
            go_tmp_filename = "/tmp/go-%d" % millis
            go_out = open(go_tmp_filename, "w")
            tmp_filenames.append(go_tmp_filename)
        go_out.write("%s %s\n" % (fp.src_path, fp.dst_path))
        go_file_length += 1
        if fp.src_host!=go_from_host or fp.dst_host!=go_to_host:
            print("Error: GO transfers only supported between one pair of hosts, but found transfers from %s to %s and from %s to %s.  Aborting." % (go_from_host, go_to_host, fp.src_host, fp.dst_host))
            sys.exit(1)

if first is False:
    pt_out.write("]")
pt_out.flush()
pt_out.close()

go_out.flush()
go_out.close()

if go_transfer is True:
    task_ids.append(goTransferSubmit(go_from_host, go_to_host, go_tmp_filename))
    #Wait on all task IDs
    for t in task_ids:
        waitTransfer(t)

rc = 0
if first is False:
    print("Calling pegasus-transfer with %s." % pt_tmp_filename)
    #Pass thru command-line args
    arg_string = " ".join(sys.argv[1:])

    cmd = "%s %s < %s" % (PEGASUS_TRANSFER, arg_string, pt_tmp_filename)
    rc = os.system(cmd)

#Delete tmp files
for f in tmp_filenames:
    if os.path.exists(f):
        os.remove(f)

sys.exit(rc)
