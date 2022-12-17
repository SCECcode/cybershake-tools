#!/bin/bash

if [ $# -lt 2 ]; then
	echo "Usage: $0 <run id> <pp execution site>"
	exit 1
fi

RUN_ID=$1
PP_SITE=$2

/home/shock/scottcal/runs/cybershake-tools/runmanager/edit_run.py ${RUN_ID} "PP_Host=${PP_SITE}"
if [ $? != 0 ]; then
        echo "Unable to update Job ID for run ${RUN_ID}"
        exit 2
fi

exit 0

