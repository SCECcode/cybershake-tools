#!/bin/bash

ROOT_RUN_DIR="/home/shock/scottcal/runs"

# Parse options
NOTIFY=""
RESTART=0
while getopts 'rnt:' OPTION
do
    case $OPTION in
        r)RESTART=1
            ;;
        n)NOTIFY="$OPTARG"   
            ;;
	t)TIMESTAMP="$OPTARG"
	    ID=$TIMESTAMP
	    ;;
        ?)printf "Usage: %s: [-r] [-n notify_user] <site run_id | -t timestamp>\n" $(basename $0) >&2
            exit 2
            ;;
    esac
done
shift $(($OPTIND - 1))

if [ $# -lt 2 ]; then
    echo "Usage: $0 [-r] [-n notify_user] <site run_id | -t timestamp>"
    exit 1
fi

if [ -z "$TIMESTAMP" ]; then
	#then we've given a site and run_id
	SITE=$1
	RUN_ID=$2
	ID=$SITE
fi

#SITES=(`echo $SITE_STRING | tr '-' ' '`)

# Ensure run table exists
RUN_FILE=${ID}_SGT_dax/run_table.txt
if [ ! -e ${RUN_FILE} ]; then
    echo "${RUN_FILE} not found"
    exit 1
else
    # Verify existing IDs are valid
    echo "Using existing run_ids from ${RUN_FILE}"
    if [ -n "$TIMESTAMP" ]; then
        echo "Checking all entries in the run file."
	#Check all the entries in the run file
	while read LINE ; do
        	RUN_ID=`echo $LINE | awk '{print $1}'`
        	SITE_NAME=`echo $LINE | awk '{print $2}'`
        	${ROOT_RUN_DIR}/cybershake-tools/runmanager/valid_run.py ${RUN_ID} ${SITE_NAME} SGT_PLAN
        	if [ $? != 0 ]; then
        	    echo "Run ${RUN_ID} not in expected state"
        	    exit 1
        	fi
    	done < ${RUN_FILE}
    else
	echo "Searching for single entry."
	#just 1 site, find the right entry
	FOUND=0
    	while read LINE ; do
        	FILE_RUN_ID=`echo $LINE | awk '{print $1}'`
        	SITE_NAME=`echo $LINE | awk '{print $2}'`
		if [ "$FILE_RUN_ID" -eq "$RUN_ID" ]; then
		        ${ROOT_RUN_DIR}/cybershake-tools/runmanager/valid_run.py ${RUN_ID} ${SITE_NAME} SGT_PLAN
        		if [ $? != 0 ]; then
        		    echo "Run ${RUN_ID} not in expected state"
        		    exit 1
        		fi
			FOUND=1
		fi
    	done < ${RUN_FILE}
    	if [ "$FOUND" -eq 0 ]; then
    	    echo "Error; run id $RUN_ID not found in $RUN_FILE."
    	    exit 2
    	fi
    fi
ID=$1
fi


# Clean out 

# Isolate pegasus run command
if [ -n "$TIMESTAMP" ]; then
	PLAN_LOG_DIR="${ID}_SGT_dax"
else
	PLAN_LOG_DIR="${ID}_SGT_dax/run_${RUN_ID}"
fi
#Added this to take the most recent log-plan file, in case we changed sites
LOG_PLAN_FILE=`ls -t ${PLAN_LOG_DIR}/log-plan-CyberShake_SGT_${ID}* | head -n 1`
PEGASUS_RUN=`grep pegasus-run ${LOG_PLAN_FILE} | head -n 1`
echo $PEGASUS_RUN
# Redirect stderr to stdout, so we can capture the job id in the tee file
${PEGASUS_RUN} 2>&1 | tee ${PLAN_LOG_DIR}/log-run-CyberShake_SGT_${ID} 

# Isolate condor jobid
JOBID=`grep "submitted to cluster" ${PLAN_LOG_DIR}/log-run-CyberShake_SGT_${ID} | head -n 1 | awk '{print $6}' | sed "s/\.//"`

# Isolate the condor submit dir
EXEC_DIR=`grep pegasus-run ${PLAN_LOG_DIR}/log-plan-CyberShake_SGT_${ID}.dax-* | cut -d" " -f3`


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

if [ -n "$TIMESTAMP" ]; then
	while read LINE ; do
	    RUN_ID=`echo $LINE | awk '{print $1}'`
	    ${ROOT_RUN_DIR}/cybershake-tools/runmanager/edit_run.py ${RUN_ID} "Status=${NEW_STATE}" "Comment=SGT top-level workflow submitted" "Job_ID=${SUBHOST}:${JOBID}" "Submit_Dir=${EXEC_DIR}" "Notify_User=${NOTIFY_MOD}"
	    if [ $? != 0 ]; then
	        echo "Unable to update Status, Comment, Job_ID, Submit_Dir, Notify_User for run ${RUN_ID}"
	        # Continue with updates
	    fi
	done < ${RUN_FILE}
else
	${ROOT_RUN_DIR}/cybershake-tools/runmanager/edit_run.py ${RUN_ID} "Status=${NEW_STATE}" "Comment=SGT workflow submitted" "Job_ID=${SUBHOST}:${JOBID}" "Submit_Dir=${EXEC_DIR}" "Notify_User=${NOTIFY_MOD}"
fi

