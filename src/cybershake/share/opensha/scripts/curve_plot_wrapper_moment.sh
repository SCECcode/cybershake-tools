#!/bin/bash

set -o errexit

/home/shock-ssd/scottcal/opensha/run_opensha.sh -Dcybershake.db.host=moment.usc.edu org.opensha.sha.cybershake.plot.HazardCurvePlotter $@
