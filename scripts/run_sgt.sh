#!/bin/bash

# Parse options
NOTIFY=""
RESTART=0
while getopts 'rn:' OPTION
do
    case $OPTION in
        r)RESTART=1
            ;;
        n)NOTIFY="$OPTARG"   
            ;;
        ?)printf "Usage: %s: [-r] [-n notify_user] <site-string>\n" $(basename $0) >&2
            exit 2
            ;;
    esac
done
shift $(($OPTIND - 1))

if [ $# -lt 1 ]; then
    echo "Usage: $0 [-r] [-n notify_user] <timestamp | site>"
    exit 1
fi

ID=$1
#SITES=(`echo $SITE_STRING | tr '-' ' '`)

# Ensure run table exists
RUN_FILE=${ID}_SGT_dax/run_table.txt
if [ ! -e ${RUN_FILE} ]; then
    echo "${RUN_FILE} not found"
    exit 1
else
    # Verify existing IDs are valid
    echo "Using existing run_ids from ${RUN_FILE}"

    while read LINE ; do
        RUN_ID=`echo $LINE | awk '{print $1}'`
        SITE_NAME=`echo $LINE | awk '{print $2}'`
        /home/scec-02/cybershk/runs/runmanager/valid_run.py ${RUN_ID} ${SITE_NAME} SGT_PLAN
        if [ $? != 0 ]; then
            echo "Run ${RUN_ID} not in expected state"
            exit 1
        fi
    done < ${RUN_FILE}
fi


# Clean out 

# Isolate pegasus run command
PEGASUS_RUN=`grep pegasus-run ${ID}_SGT_dax/log-plan-CyberShake_SGT_${ID}* | head -n 1`
echo $PEGASUS_RUN
${PEGASUS_RUN} | tee ${ID}_SGT_dax/log-run-CyberShake_SGT_${ID} 

# Isolate condor jobid
JOBID=`grep "submitted to cluster" ${ID}_SGT_dax/log-run-CyberShake_SGT_${ID} | head -n 1 | awk '{print $6}' | sed "s/\.//"`

# Isolate the condor submit dir
EXEC_DIR=`grep pegasus-run ${ID}_SGT_dax/log-plan-CyberShake_SGT_${ID}.dax-* | cut -d" " -f3`


# Submit jobid to condor watch if monitoring enabled
if [ "${NOTIFY}" != "" ]; then
    ../conwatch/Watch.py ${JOBID} "${ID} SGT Workflow" ${NOTIFY}
fi


# Update the run record with new status, comment, job_id, submit_dir, notify_user
# Assigns the parent dag job_id to all runs
SUBHOST=`hostname -s`
if [ "${NOTIFY}" != "" ]; then
    NOTIFY_MOD="${NOTIFY}"
else
    NOTIFY_MOD="NULL"
fi
if [ ${RESTART} -eq 1 ]; then
    NEW_STATE="SGT Started"
else
    NEW_STATE="Initial"
fi

while read LINE ; do
    RUN_ID=`echo $LINE | awk '{print $1}'`
    /home/scec-02/cybershk/runs/runmanager/edit_run.py ${RUN_ID} "Status=${NEW_STATE}" "Comment=SGT PDAX workflow submitted" "Job_ID=${SUBHOST}:${JOBID}" "Submit_Dir=${EXEC_DIR}" "Notify_User=${NOTIFY_MOD}"
    if [ $? != 0 ]; then
        echo "Unable to update Status, Comment, Job_ID, Submit_Dir, Notify_User for run ${RUN_ID}"
        # Continue with updates
    fi
done < ${RUN_FILE}
