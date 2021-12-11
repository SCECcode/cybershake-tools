#!/bin/bash

#Process usually takes ~5 sec. Runs every 15 min, so kill after 5.
timeout 300s /home/shock/scottcal/runs/cybershake-tools/runmanager/cron_run_check.py $@
RC=$?
if [ "$RC" -eq 124 ]; then
	echo "Cron check didn't terminate naturally, so it was killed."
	rm /home/shock/scottcal/runs/cybershake-tools/runmanager/lock
fi
