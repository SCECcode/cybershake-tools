#!/bin/bash

if [ $# -lt 1 ]; then
    echo "Usage: $0 <site> [notify user]"
    exit 1
fi

SITE=$1

NOTIFY_FILE="${SITE}_SGT_notify.list"
if [ $# -eq 2 ]; then
    NOTIFY=$2
    echo "Saving ${NOTIFY} to the notify list ${NOTIFY_FILE}"
    echo "${NOTIFY}" > ./notify/${NOTIFY_FILE}
else
    NOTIFY="none"
    echo "Disabling email notification"
    echo "" > ./notify/${NOTIFY_FILE}
fi


# Isolate pegasus run command
PEGASUS_RUN_1=`ls -t ${SITE}_SGT_dax/log-plan-CyberShake_SGT_${SITE}* | head -n 1`
PEGASUS_RUN=`more $PEGASUS_RUN_1 | grep pegasus-run | head -n 1`
echo $PEGASUS_RUN
${PEGASUS_RUN} | tee ${SITE}_SGT_dax/log-run-CyberShake_SGT_${SITE}

# Isolate condor jobid
JOBID=`grep "submitted to cluster" ${SITE}_SGT_dax/log-run-CyberShake_SGT_${SITE} | head -n 1 | awk '{print $6}' | sed "s/\.//"`

# Submit jobid to condor watch if monitoring enabled
if [ ${NOTIFY} != "none" ]; then
    ../conwatch/Watch.py ${JOBID} "${SITE} SGT Workflow" ${NOTIFY}
fi
