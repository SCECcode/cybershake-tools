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
	?)printf "Usage: %s: [-r] [-n notify_user] <site>\n" $(basename $0) >&2
	    exit 2
	    ;;
    esac
done
shift $(($OPTIND - 1))

if [ $# -lt 1 ]; then
    echo "Usage: $0 [-r] [-n notify_user] <site>"
    exit 1
fi

SITE=$1

# Ensure run table exists
RUN_FILE=${SITE}_PP_dax/run_table.txt
if [ ! -e ${RUN_FILE} ]; then
    echo "${RUN_FILE} not found"
    exit 1
else
    # Verify existing IDs are valid
    echo "Using existing run_ids from ${RUN_FILE}"

    while read LINE ; do
        RUN_ID=`echo $LINE | awk '{print $1}'`
        SITE_NAME=`echo $LINE | awk '{print $2}'`
        /home/scec-02/cybershk/runs/runmanager/valid_run.py ${RUN_ID} ${SITE_NAME} PP_PLAN
        if [ $? != 0 ]; then
            echo "Run ${RUN_ID} not in expected state"
            exit 1
        fi
    done < ${RUN_FILE}
fi



# Isolate the pegasus run command
PEGASUS_RUN=`grep pegasus-run ${SITE}_PP_dax/log-plan-CyberShake_${SITE}.dax-*`
which pegasus-run
echo ${PEGASUS_RUN}
${PEGASUS_RUN} | tee ${SITE}_PP_dax/log-run-CyberShake_PP_${SITE}.dax


# Isolate condor jobid
JOBID=`grep "submitted to cluster" ${SITE}_PP_dax/log-run-CyberShake_PP_${SITE}.dax | head -n 1 | awk '{print $6}' | sed "s/\.//"`


# Isolate the condor submit dir
EXEC_DIR=`grep pegasus-run ${SITE}_PP_dax/log-plan-CyberShake_${SITE}.dax-* | cut -d" " -f3`


# Save condor_config.local
if [ -n "${CONDOR_CONFIG+x}" ]; then
        CFG=${CONDOR_CONFIG}.local
elif [ -e /etc/condor/condor_config.local ]; then
        CFG=/etc/condor/condor_config.local
elif [ -e ~condor/condor_config.local ]; then
        CFG=~condor/condor_config.local
elif [ -e ${GLOBUS_LOCATION}/etc/condor_config ]; then
        CFG=${GLOBUS_LOCATION}/etc/condor_config
else
        echo "No condor config file found."
        exit 1
fi
echo "Condor config local $CFG" >> ${EXEC_DIR}/metadata.local
more $CFG >> ${EXEC_DIR}/metadata.local


# Submit jobid to condor watch if monitoring enabled
if [ "${NOTIFY}" != "" ]; then
    conwatch/Watch.py ${JOBID} "${SITE} PP Workflow" ${NOTIFY}
fi


# Update run record to include comment, job_id, submit_dir, notify_user
SUBHOST=`hostname -s`
if [ "${NOTIFY}" != "" ]; then
    NOTIFY_MOD="${NOTIFY}"
else
    NOTIFY_MOD="NULL"
fi
if [ ${RESTART} -eq 1 ]; then
    NEW_STATE="PP Started"
else
    NEW_STATE="SGT Generated"
fi

while read LINE ; do
    RUN_ID=`echo $LINE | awk '{print $1}'`
    /home/scec-02/cybershk/runs/runmanager/edit_run.py ${RUN_ID} "Status=${NEW_STATE}" "Comment=PP workflow submitted" "Job_ID=${SUBHOST}:${JOBID}" "Submit_Dir=${EXEC_DIR}" "Notify_User=${NOTIFY_MOD}"
    if [ $? != 0 ]; then
        echo "Unable to update Status, Comment, Job_ID, Submit_Dir, Comment for run ${RUN_ID}"
        # Continue with updates
    fi
done < ${RUN_FILE}
