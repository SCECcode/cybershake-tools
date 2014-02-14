#!/bin/bash

if [ $# -lt 1 ]; then
    echo "Usage: $0 <site> [notify_user]"
    exit 1
fi

SITE=$1

NOTIFY_FILE="${SITE}_PP_notify.list"
if [ $# -eq 2 ]; then
    NOTIFY=$2
    echo "Saving ${NOTIFY} to the notify list ${NOTIFY_FILE}"
    echo "${NOTIFY}" > ./notify/${NOTIFY_FILE}
else
    NOTIFY="none"
    echo "Disabling email notification"
    echo "" > ./notify/${NOTIFY_FILE}
fi


# Isolate the pegasus run command
PEGASUS_RUN=`grep pegasus-run ${SITE}_PP_dax/log-plan-CyberShake_${SITE}.pdax-*`
${PEGASUS_RUN} | tee ${SITE}_PP_dax/log-run-CyberShake_PP_${SITE}.pdax

# Isolate condor jobid
JOBID=`grep "submitted to cluster" ${SITE}_PP_dax/log-run-CyberShake_PP_${SITE}.pdax | head -n 1 | awk '{print $6}' | sed "s/\.//"`

# Submit jobid to condor watch if monitoring enabled
if [ ${NOTIFY} != "none" ]; then
    ../conwatch/Watch.py ${JOBID} "${SITE} PP Workflow" ${NOTIFY}
fi
