#!/usr/bin/env python3

"""Creates an SQLite file with the PeakAmps that will be needed for the curve calc"""

import argparse
import shutil
import sqlite3
import sys

# Command like:
# /home/shock-ssd/scottcal/opensha/curve_plot_wrapper.sh --site s3240   --run-id 10728   --erf-file /home/shock-ssd/scottcal/opensha/conf/MeanUCERF.xml   --atten-rel-file /home/shock-ssd/scottcal/opensha/conf/ask2014.xml,/home/shock-ssd/scottcal/opensha/conf/bssa2014.xml,/home/shock-ssd/scottcal/opensha/conf/cb2014.xml,/home/shock-ssd/scottcal/opensha/conf/cy2014.xml   --period 3,4,5,7.5,10,2   --output-dir /home/shock/scottcal/db_products/curves/s3240   --type pdf,png   --force-add   --cmp RotD50   --password-file /home/shock/scottcal/runs/config/db_pass.txt   --plot-chars-file /home/shock-ssd/scottcal/opensha/conf/robPlot.xml

parser = argparse.ArgumentParser()
parser.add_argument("-s", "--site", type=str)
parser.add_argument("-R", "--run-id", type=int)
parser.add_argument("-p", "--period", type=str)
parser.add_argument("-cmp", "--component", type=str)
# parser.add_argument("-db", "--database", type=str)

args, unknown = parser.parse_known_args()
print(args)

if (
    args.site == None
    or args.run_id == None
    or args.period == None
    or args.component == None
):
    print("Couldn't find site, run_id, periods, or component.")
    sys.exit(1)

site = args.site
run_id = args.run_id
period = args.period
component = args.component
# database = "moment"
# if args.database is not None:
#    database = args.database

# if database.find("sqlite:")==0:
#    db_filename = database.split("sqlite:")[1]
#    print("Connecting to sqlite db %s" % db_filename)
#    conn = sqlite3.connect(db_filename)
# else:
#    conn = pymysql.connect(host='moment', db='CyberShake', user='cybershk_ro', passwd="CyberShake2007")
conn = sqlite3.connect(
    "/scratch1/scottcal/cybershake/study_22_12_lf_rerun_indexed.sqlite"
)
cur = conn.cursor()

# Copy in base sqlite DB
BASE_SQLITE_DB = "base_curve.sqlite"
site_sqlite_db = "tmp.sqlite"
shutil.copyfile(BASE_SQLITE_DB, site_sqlite_db)


sqlite_conn = sqlite3.connect(site_sqlite_db)
sqlite_cur = sqlite_conn.cursor()

query = "select * from CyberShake_Runs where Run_ID=%d" % (run_id)
cur.execute(query)
res = cur.fetchone()
str_res = [str(r) for r in res]
insert = 'insert into CyberShake_Runs values ("%s")' % '","'.join(str_res)
print(insert)
sqlite_cur.execute(insert)
sqlite_conn.commit()

periods_list = period.split(",")
for p in periods_list:
    query = (
        "select P.Source_ID, P.Rupture_ID, P.Rup_Var_ID, P.Run_ID, P.IM_Type_ID, P.IM_Value from PeakAmplitudes P, IM_Types I where P.Run_ID=%d and I.IM_Type_Value=%s and I.IM_Type_ID=P.IM_Type_ID and I.IM_Type_Component='%s'"
        % (run_id, p, component)
    )
    print(query)
    cur.execute(query)
    res = cur.fetchall()
    for r in res:
        insert = (
            "insert into PeakAmplitudes (Source_ID, Rupture_ID, Rup_Var_ID, Run_ID, IM_Type_ID, IM_Value) values (%d, %d, %d, %d, %d, %f)"
            % (r[0], r[1], r[2], r[3], r[4], r[5])
        )
        sqlite_cur.execute(insert)
    sqlite_conn.commit()

conn.close()
sqlite_conn.close()
