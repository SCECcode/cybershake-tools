#!/usr/bin/env python3

'''This script takes a file produced by velocity_params.py, as part of the SGT calculation, and puts it into the database.  This is used because Frontier compute nodes don't have external network access, and also because moment-carc is behind a firewall.'''

import sys
import os

sys.path.append("/home1/scottcal/.local/lib/python3.9/site-packages")

import pymysql
import argparse

parser = argparse.ArgumentParser(description = "This script takes a file produced by velocity_params.py, generated as part of the SGT workflow, and inserts it into the database on moment-carc.")
parser.add_argument("-i", "--input_file", help="Path to input file (required).")
parser.add_argument("-r", "--run_id", help="Run ID (required).", type=int)
parser.add_argument("-d", "--db_file", help="Path to file with database configuration information (required).")
parser.add_argument("-s", "--server", help="Database server to insert velocity values into (default is moment-carc).")

args = parser.parse_args()
if args.input_file==None or args.run_id==None or args.db_file==None:
    print("Missing required arguments.")
    parser.print_help()
    sys.exit(1)

input_file = args.input_file
run_id = args.run_id
db_file = args.db_file
server = "moment"
if args.server is not None:
    server = args.server

with open(input_file, "r") as fp_in:
    model_vs30 = float(fp_in.readline().split("=")[1])
    mesh_vs = float(fp_in.readline().split("=")[1])
    model_z10 = float(fp_in.readline().split("=")[1])
    model_z25 = float(fp_in.readline().split("=")[1])
    fp_in.close()


with open(db_file, "r") as fp_in:
    pieces = fp_in.readline().split(":")
    username = pieces[0]
    password = pieces[1].strip()
    fp_in.close()

conn = pymysql.connect(host=server, db="CyberShake", user=username, passwd=password)
cur = conn.cursor()
update = "update CyberShake_Runs set Model_Vs30=%f, Mesh_Vsitop=%f, Z1_0=%f, Z2_5=%f where Run_ID=%d" % (model_vs30, mesh_vs, model_z10, model_z25, int(args.run_id))
print(update)
cur.execute(update)
conn.commit()
cur.close()

sys.exit(0)
