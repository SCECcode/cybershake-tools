#!/bin/bash

cd /home/shock-ssd/scottcal/opensha
./run_opensha.sh -Xmx12g -Dcybershake.db.host=moment.usc.edu org.opensha.sha.cybershake.plot.DisaggregationPlotter $@
