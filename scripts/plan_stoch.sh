#!/bin/bash

if [ $# -ne 3 ]; then
        echo "Usage: $0 <site> <run_id> <remote_site>"
        echo "Example: $0 USC 135 abe"
        exit 1
fi

SITE=$1
RUN_ID=$2
REMOTE_SITE=$3

DAX=CyberShake_Stoch_${SITE}_top.dax

cd ${SITE}_Stoch_dax

# Check that run table exists
RUN_FILE=run_table.txt
if [ ! -e ${RUN_FILE} ]; then
    echo "${RUN_FILE} not found"
    exit 1
else
    # Verify existing IDs are valid
    echo "Using existing run_ids from ${RUN_FILE}"

    FOUND=0
    while read LINE ; do
        FILE_RUN_ID=`echo $LINE | awk '{print $1}'`
        SITE_NAME=`echo $LINE | awk '{print $2}'`
        if [ "$FILE_RUN_ID" -eq "$RUN_ID" ]; then
                /home/scec-02/cybershk/runs/runmanager/valid_run.py ${RUN_ID} ${SITE_NAME} PP_PLAN
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

if [ "${REMOTE_SITE}" == "bluewaters" ]; then
        echo "Using bluewaters propfile"
        export X509_USER_PROXY=/tmp/x509up_u33527
        propfile=/home/scec-02/cybershk/runs/config/properties.bluewaters
else
        export X509_USER_PROXY=/tmp/x509up_u801878
fi

rm run_${RUN_ID}/log-plan-${DAX}-*
cd run_${RUN_ID}
echo pegasus-plan --conf ${propfile} --rescue 100 --dax ${DAX} -s $REMOTE_SITE,shock,opensha --output-site shock --dir dags -f --cluster label --nocleanup | tee log-plan-${DAX}-${REMOTE_SITE}
pegasus-plan --conf ${propfile} --rescue 100 --dax ${DAX} -s $REMOTE_SITE,shock,opensha --output-site shock --dir dags -f --cluster label --nocleanup | tee log-plan-${DAX}-${REMOTE_SITE}

# Modify the run-time user properties file
# Get directory and look for propfile
line=`grep pegasus-run log-plan-${DAX}-${REMOTE_SITE} | cut -d" " -f3`
propfile=`ls ${line}/pegasus.*.properties`
cp $propfile ${propfile}.top
sed -i "s|pegasus.exitcode.scope=all|pegasus.exitcode.scope=none|g" ${propfile}.top

sed -i "s|${propfile}|${propfile}.top|g" log-plan-${DAX}-${REMOTE_SITE}

#Put exitcodes back in the subdax properties files
sed -i 's/pegasus.exitcode.scope=none/pegasus.exitcode.scope=all/g' $propfile

#dump TC into metadata file
LINE=`grep pegasus.catalog.transformation.file $propfile`
TC_PATH=${LINE#*=}
EXEC_DIR=`grep pegasus-run log-plan-${DAX}-${REMOTE_SITE} | cut -d" " -f3`
echo "Site $REMOTE_SITE" > ${EXEC_DIR}/metadata.local
echo "TC $TC_PATH" >> ${EXEC_DIR}/metadata.local
more $TC_PATH >> ${EXEC_DIR}/metadata.local

#put the sqlite DB into the run directory
if [ -e ${SITE_NAME}.db ]; then
        mv ${SITE_NAME}.db ${EXEC_DIR}/${SITE_NAME}_${RUN_ID}.db
fi

/home/scec-02/cybershk/runs/runmanager/edit_run.py ${RUN_ID} "PP_Host=${REMOTE_SITE}" "Job_ID=NULL"
if [ $? != 0 ]; then
        echo "Unable to update Status, PP_Host, Job_ID for run ${RUN_ID}"
        exit 1
fi

