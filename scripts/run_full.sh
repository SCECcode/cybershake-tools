#!/bin/bash

# Parse options
NOTIFY=""
RESTART=0
RESTART_SGT=0
RESTART_PP=0

ROOT_RUN_DIR="/home/shock/scottcal/runs"

if [ $# -lt 2 ]; then
    echo "Usage: $0 [-r] [-n notify_user] <site run_id | -t timestamp>"
    exit 1
fi

while getopts 'rn:t:' OPTION
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

if [ -z "$TIMESTAMP" ]; then
	SITE=$1
	RUN_ID=$2
	ID=$SITE
	echo $SITE $RUN_ID
fi

#SITES=(`echo $SITE_STRING | tr '-' ' '`)

# Ensure run table exists
RUN_FILE=${ID}_Integrated_dax/run_table.txt
if [ ! -e ${RUN_FILE} ]; then
    echo "${RUN_FILE} not found"
    exit 1
else
    # Verify existing IDs are valid
    echo "Using existing run_ids from ${RUN_FILE}"

    if [ -n "$TIMESTAMP" ]; then
	RUN_DIR=${ID}_Integrated_dax
	echo "Checking all entries in the run file."
	while read LINE ; do
        	RUN_ID=`echo $LINE | awk '{print $1}'`
        	SITE_NAME=`echo $LINE | awk '{print $2}'`
        	${ROOT_RUN_DIR}/cybershake-tools/runmanager/valid_run.py ${RUN_ID} ${SITE_NAME} SGT_PLAN
        	if [ $? != 0 ]; then
		    ${ROOT_RUN_DIR}/cybershake-tools/runmanager/valid_run.py ${RUN_ID} ${SITE_NAME} PP_PLAN
		    if [ $? != 0 ]; then
		            echo "Run ${RUN_ID} not in expected state"
		            exit 1
		    else
			    RESTART_PP=1
		    fi
        	else
		    if [ ${RESTART} -eq 1 ]; then
			    RESTART_SGT=1
		    fi
        	fi
	done < ${RUN_FILE}
    else
	RUN_DIR=${ID}_Integrated_dax/run_${RUN_ID}
	echo "Searching for single entry."
	FOUND=0
	while read LINE ; do
        	FILE_RUN_ID=`echo $LINE | awk '{print $1}'`
                SITE_NAME=`echo $LINE | awk '{print $2}'`
                if [ "$FILE_RUN_ID" -eq "$RUN_ID" ]; then
					${ROOT_RUN_DIR}/cybershake-tools/runmanager/valid_run.py ${RUN_ID} ${SITE_NAME} SGT_PLAN
               		if [ $? != 0 ]; then
                    		${ROOT_RUN_DIR}/cybershake-tools/runmanager/valid_run.py ${RUN_ID} ${SITE_NAME} PP_PLAN
                    		if [ $? != 0 ]; then
                        	    echo "Run ${RUN_ID} not in expected state"
                        	    exit 1
                    		else
                        	    RESTART_PP=1
                    		fi
                	else
                	    if [ ${RESTART} -eq 1 ]; then
                	            RESTART_SGT=1
                	    fi
                	fi
			FOUND=1
			if [ -f $RUN_DIR/linked_bb_run_id.txt ]; then
				BB_RUN_ID=`cat $RUN_DIR/linked_bb_run_id.txt`
				${ROOT_RUN_DIR}/cybershake-tools/runmanager/valid_run.py ${BB_RUN_ID} ${SITE_NAME} SGT_PLAN
				if [ $? != 0 ]; then
					${ROOT_RUN_DIR}/cybershake-tools/runmanager/valid_run.py ${BB_RUN_ID} ${SITE_NAME} PP_PLAN
					if [ $? != 0 ]; then
                                    		echo "Run ${BB_RUN_ID} not in expected state"
                                    		exit 1
					fi
				fi
			fi
		fi
	done < ${RUN_FILE}
	if [ "$FOUND" -eq 0 ]; then
		echo "Error; run id $BB_RUN_ID not found in $RUN_FILE."
		exit 2
	fi
    fi
fi


# Clean out 

# Isolate pegasus run command


PEGASUS_RUN=`grep pegasus-run ${RUN_DIR}/log-plan-CyberShake_Integrated_${ID}* | head -n 1`
echo $PEGASUS_RUN
# Redirect stderr to stdout, so we can capture the job id in the tee file
${PEGASUS_RUN} 2>&1 | tee ${RUN_DIR}/log-run-CyberShake_Integrated_${ID} 

# Isolate condor jobid
JOBID=`grep "submitted to cluster" ${RUN_DIR}/log-run-CyberShake_Integrated_${ID} | head -n 1 | awk '{print $6}' | sed "s/\.//"`

# Isolate the condor submit dir
EXEC_DIR=`grep pegasus-run ${RUN_DIR}/log-plan-CyberShake_Integrated_${ID}.dax-* | cut -d" " -f3`


# Submit jobid to condor watch if monitoring enabled
if [ "${NOTIFY}" != "" ]; then
    if [ $RESTART_SGT -eq 1 ]; then
      ../conwatch/Watch.py ${JOBID} "${ID} SGT Workflow" ${NOTIFY}
    elif [ $RESTART_PP -eq 1 ]; then
      ../conwatch/Watch.py ${JOBID} "${ID} PP Workflow" ${NOTIFY}
    fi
fi


# Update the run record with new status, comment, job_id, submit_dir, notify_user
# Assigns the parent dag job_id to all runs
SUBHOST=`hostname -s`
if [ "${NOTIFY}" != "" ]; then
    NOTIFY_MOD="${NOTIFY}"
else
    NOTIFY_MOD="NULL"
fi
if [ ${RESTART_SGT} -eq 1 ]; then
    NEW_STATE="SGT Started"
elif [ ${RESTART_PP} -eq 1 ]; then
    NEW_STATE="PP Started"
else
    NEW_STATE="Initial"
fi

if [ -n "$TIMESTAMP" ]; then
	while read LINE ; do
	    RUN_ID=`echo $LINE | awk '{print $1}'`
	    ${ROOT_RUN_DIR}/cybershake-tools/runmanager/edit_run.py ${RUN_ID} "Status=${NEW_STATE}" "Comment=workflow submitted" "Job_ID=${SUBHOST}:${JOBID}" "Submit_Dir=${EXEC_DIR}" "Notify_User=${NOTIFY_MOD}"
	    if [ $? != 0 ]; then
	        echo "Unable to update Status, Comment, Job_ID, Submit_Dir, Notify_User for run ${RUN_ID}"
	        # Continue with updates
	    fi
	done < ${RUN_FILE}
else
	${ROOT_RUN_DIR}/cybershake-tools/runmanager/edit_run.py ${RUN_ID} "Status=${NEW_STATE}" "Comment=workflow submitted" "Job_ID=${SUBHOST}:${JOBID}" "Submit_Dir=${EXEC_DIR}" "Notify_User=${NOTIFY_MOD}"
fi

if [ -f $RUN_DIR/linked_bb_run_id.txt ]; then
	BB_RUN_ID=`cat $RUN_DIR/linked_bb_run_id.txt`
	${ROOT_RUN_DIR}/cybershake-tools/runmanager/edit_run.py ${BB_RUN_ID} "Status=${NEW_STATE}" "Comment=workflow submitted" "Job_ID=${SUBHOST}:${JOBID}" "Submit_Dir=${EXEC_DIR}" "Notify_User=${NOTIFY_MOD}"
fi

