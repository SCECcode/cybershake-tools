#!/usr/bin/env python3

import sqlite3
import sys

import pymysql

# /home/shock-ssd/scottcal/opensha/curve_plot_wrapper.sh --site s3240   --run-id 10728   --erf-file /home/shock-ssd/scottcal/opensha/conf/MeanUCERF.xml   --atten-rel-file /home/shock-ssd/scottcal/opensha/conf/ask2014.xml,/home/shock-ssd/scottcal/opensha/conf/bssa2014.xml,/home/shock-ssd/scottcal/opensha/conf/cb2014.xml,/home/shock-ssd/scottcal/opensha/conf/cy2014.xml   --period 3,4,5,7.5,10,2   --output-dir /home/shock/scottcal/db_products/curves/s3240   --type pdf,png   --force-add   --cmp RotD50   --password-file /home/shock/scottcal/runs/config/db_pass.txt   --plot-chars-file /home/shock-ssd/scottcal/opensha/conf/robPlot.xml

parser = argparse.ArgumentParser()
parser.add_argument("-s", "--site", type=str)
parser.add_argument("-R", "--run-id", type=int)
parser.add_argument("-p", "--periods", type=str)
parser.add_argument("-cmp", "--component", type=str)

args, unknown = parser.parse_known_args()
print(args)

if (
    args.site == None
    or args.run_id == None
    or args.periods == None
    or args.component == None
):
    print("Couldn't find site, run_id, periods, or component.")
    sys.exit(1)

site = args.site
run_id = args.run_id
periods = args.periods
component = args.component

conn = pymysql.connect(
    host="moment", db="CyberShake", user="cybershk", passwd="phy$ic@1St3ady"
)
cur = conn.cursor()

sqlite_conn = sqlite3.connect("tmp.sqlite")
sqlite_cur = sqlite_conn.cursor()

period_list = periods.split(",")
for p in period_list:
    query = (
        "select C.Hazard_Curve_ID, C.Run_ID, C.Curve_Date, C.IM_Type_ID, C.Hazard_Dataset_ID from Hazard_Curves C, IM_Types I where I.IM_Type_ID=C.IM_Type_ID and C.Run_ID=%d and I.IM_Type_Value=%f and I.IM_Type_Component='%s'"
        % (run_id, p, component)
    )
    sqlite_cur.execute(query)
    res = sqlite_cur.fetchone()
    insert = (
        "insert into Hazard_Curves (Run_ID, Curve_Date, IM_Type_ID, Hazard_Dataset_ID) values (%d, %s, %d, %d)"
        % (res[1], res[2], res[3], res[4])
    )
    cur.execute(insert)
    conn.commit()
    # Need to get the curve ID we just inserted
    query = (
        "select Hazard_Curve_ID from Hazard_Curves where Run_ID=%d order by Hazard_Curve_ID desc limit 1"
        % run_id
    )
    cur.execute(query)
    curve_id = int(cur.fetchone()[0])
    # Now do points
    query = (
        "select P.X_Value, P.Y_Value from Hazard_Curve_Points P, Hazard_Curves C, IM_Types I where I.IM_Type_ID=C.IM_Type_ID and I.IM_Type_Value=%f and I.IM_Type_Component='%s' and C.Run_ID=%d and C.Hazard_Curve_ID=P.Hazard_Curve_ID"
        % (p, component, run_id)
    )
    sqlite_cur.execute(query)
    res = sqlite_cur.fetchall()
    for r in res:
        insert = (
            "insert into Hazard_Curve_Points (Hazard_Curve_ID, X_Value, Y_Value) values (%d, %f, %f)"
            % (curve_id, r[0], r[1])
        )
        cur.execute(insert)
    conn.commit()

conn.close()
sqlite_conn.close()
