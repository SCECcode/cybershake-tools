#!/bin/bash

if [ $# -ne 4 ]; then
	echo "Usage: $0 <site> <remote_site> <gridshib user> <gridshib user email>"
	echo "Example: $0 USC abe scottcal scottcal@usc.edu"
	exit 1
fi

SITE=$1
REMOTE_SITE=$2
USER=$3
EMAIL=$4

#initialize saml certificate
/home/scec-00/cybershk/gridshib/issue-saml-cert.sh $USER $EMAIL
NEW_PROXY=/tmp/${USER}_gridshib_proxy.pem

export X509_USER_PROXY=$NEW_PROXY

DAX=CyberShake_${SITE}.dax

cd ${SITE}_PP_dax


# Check that run table exists
RUN_FILE=run_table.txt
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


# Modify the planning properties file
propfile=/home/scec-02/cybershk/runs/config/properties.mpi-cluster.pp
cp $propfile properties.top
while read LINE ; do
    RUN_ID=`echo $LINE | awk '{print $1}'`
    echo "pegasus.dir.storage=data/PPFiles/${SITE}/${RUN_ID}" >> properties.top
done < ${RUN_FILE}
newpropfile=`pwd`/properties.top


# Remove any old plan logs
rm log-plan-${DAX}-*

# Perform the planning
echo "pegasus-plan -Dpegasus.local.env=X509_USER_PROXY=${NEW_PROXY} --conf ${newpropfile} --rescue 100 --dax ${DAX} -s $REMOTE_SITE,shock,opensha -o shock --dir dags -f --cluster label --nocleanup"
which pegasus-plan
#pegasus-plan -Dpegasus.local.env=X509_USER_PROXY=${NEW_PROXY} --conf ${newpropfile} --rescue 100 --dax ${DAX} -s $REMOTE_SITE,shock,opensha -o shock --dir dags -f --cluster horizontal --nocleanup | tee log-plan-${DAX}-${REMOTE_SITE}
pegasus-plan -Dpegasus.local.env=X509_USER_PROXY=${NEW_PROXY} --conf ${newpropfile} --rescue 100 --dax ${DAX} -s $REMOTE_SITE,shock,opensha -o shock --dir dags -f --cluster label --nocleanup | tee log-plan-${DAX}-${REMOTE_SITE}


# Modify the run-time user properties file
# Get directory and look for propfile
line=`grep pegasus-run log-plan-${DAX}-${REMOTE_SITE} | cut -d" " -f3`
propfile=`ls ${line}/pegasus.*.properties`
cp $propfile ${propfile}.top
echo "pegasus.dagman.maxjobs=20" >> ${propfile}.top
sed -i "s|pegasus.exitcode.scope=all|pegasus.exitcode.scope=none|g" ${propfile}.top

sed -i "s|${propfile}|${propfile}.top|g" log-plan-${DAX}-${REMOTE_SITE}

#Put exitcodes back in the subdax properties files
sed -i 's/pegasus.exitcode.scope=none/pegasus.exitcode.scope=all/g' $propfile

# Ranger-specific:  Modify the ID0 properties file to enable third-party transfer, b/c we need to transfer the notify file from HPC and if we try push/pull we run into issues because of the version of gridftp on HPC.  We could use shock, but since we have to do the stage-outs it could be too much load.
dag=$(dirname $propfile)/CyberShake_${SITE}-0.dag
cp $propfile ${propfile}_ID0
echo "pegasus.transfer.stageout.thirdparty.sites = *" >> ${propfile}_ID0
echo "pegasus.transfer.stagein.thirdparty.sites = ranger" >> ${propfile}_ID0
echo "pegasus.transfer.*.thirdparty.remote = ranger" >> ${propfile}_ID0
sed -i 's|\(SCRIPT PRE 00/ID0.*\)\(pegasus\..*\.properties\)|\1\2_ID0|g' $dag

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

# Update the run record
while read LINE ; do
    RUN_ID=`echo $LINE | awk '{print $1}'`
    /home/scec-02/cybershk/runs/runmanager/edit_run.py ${RUN_ID} "Status=SGT Generated" "PP_Host=${REMOTE_SITE}" "Comment=Planned, assigned pp host" "Job_ID=NULL"
    if [ $? != 0 ]; then
        echo "Unable to update Status, PP_Host, Job_ID for run ${RUN_ID}"
        exit 1
    fi
done < ${RUN_FILE}
