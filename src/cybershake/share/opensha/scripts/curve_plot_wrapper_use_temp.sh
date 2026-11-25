#!/bin/bash

#This script pulls the PeakAmps into a SQLite file, runs curve calc on it, and then populates the database with the curve details.  It's to get around issues with DB authentication in OpenSHA.

set -o errexit

/home/shock-ssd/scottcal/opensha/create_sqlite.py $@

/home/shock-ssd/scottcal/opensha/run_opensha.sh -Dcybershake.db.host=/home/shock-ssd/scottcal/opensha/tmp.sqlite org.opensha.sha.cybershake.plot.HazardCurvePlotter $@

/home/shock-ssd/scottcal/opensha/pop_curves_from_sqlite.py
