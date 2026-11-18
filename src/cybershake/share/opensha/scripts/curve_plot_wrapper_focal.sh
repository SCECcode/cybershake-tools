#!/bin/bash

/home/shock-ssd/scottcal/opensha/run_opensha.sh -Djava.awt.headless=true -Dcybershake.db.host=focal.usc.edu org.opensha.sha.cybershake.plot.HazardCurvePlotter $@
