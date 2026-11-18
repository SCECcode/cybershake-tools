#!/bin/bash

cd /home/shock-ssd/scottcal/opensha
./run_opensha.sh -Xmx12g -Dcybershake.db.host=moment org.opensha.sha.cybershake.plot.DisaggregationPlotter $@
