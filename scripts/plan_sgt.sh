#!/bin/sh

if [ $# -lt 2 ]; then
	echo "Usage:  $0 <SGT dir> remote_site"
	echo "Example: $0  1234543210_SGT_dax ranger"
	exit 1
fi

DAX_DIR=$1
REMOTE_SITE=$2

cd $DAX_DIR
TOP_DAX=""
TWO_LEVELS=0
TIMESTAMP=""

#determine if we have 1 site or many
if [ `more sites.list | wc -l` -gt 1 ]; then
	#more than 1 site
	TIMESTAMP=${DAX_DIR%%_*}
	TOP_DAX=CyberShake_SGT_${TIMESTAMP}.dax
	TWO_LEVELS=1
	#SITE_PART=${PDAX_DIR%%_*}
	#PDAX="CyberShake_SGT_${SITE_PART}.pdax"
	#PDAX=`ls CyberShake_SGT_*.pdax`
else
	SITE=${DAX_DIR%%_*}
	TOP_DAX=CyberShake_SGT_${SITE}.dax
fi

#if we use HPC, we need to convert RLS parameters
#host_count = host_xcount
#count = xcount * host_xcount
#if [ $REMOTE_SITE == "hpc" ]; then
#	sed -i 's/xcount/count/g' $DAX
#fi


# Verify the run table
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
        /home/scec-02/cybershk/runs/runmanager/valid_run.py ${RUN_ID} ${SITE_NAME} SGT_PLAN
        if [ $? != 0 ]; then
            echo "Run ${RUN_ID} not in expected state"
            exit 1
        fi
    done < ${RUN_FILE}

fi

# Remove any old plan logs
rm log-plan-${TOP_DAX}-*

# Modify properties file if 2 levels
propfile=/home/scec-02/cybershk/runs/config/properties.sgt
if [ $TWO_LEVELS -eq 1 ]; then
	cp $propfile properties.sgt.top
	echo "pegasus.dir.storage=data/SgtFiles/${TIMESTAMP}" >> properties.sgt.top
	echo "pegasus.dagman.maxjobs=15" >> properties.sgt.top
	newpropfile=`pwd`/properties.sgt.top
	echo "pegasus-plan --conf=${newpropfile} --dax $TOP_DAX --rescue 100 -s $REMOTE_SITE,shock -o ${REMOTE_SITE} --dir dags -f --nocleanup | tee log-plan-${TOP_DAX}-${REMOTE_SITE}"
	pegasus-plan --conf=${newpropfile} --dax $TOP_DAX --rescue 100 -s $REMOTE_SITE,shock -o $REMOTE_SITE --dir dags -f --nocleanup | tee log-plan-${TOP_DAX}-${REMOTE_SITE}
	if [ $? != 0 ]; then
	    echo "Failed to plan workflow."
	    exit 1
	fi
else
	cp $propfile properties.sgt.onesite
        echo "pegasus.dir.storage=data/SgtFiles/${SITE}" >> properties.sgt.onesite
	echo "pegasus-plan --conf=${propfile} --dax $TOP_DAX --rescue 100 -s $REMOTE_SITE,shock -o ${REMOTE_SITE} --dir dags -f --nocleanup | tee log-plan-${TOP_DAX}-${REMOTE_SITE}"
        pegasus-plan --conf=properties.sgt.onesite --dax $TOP_DAX --rescue 100 -s $REMOTE_SITE,shock -o $REMOTE_SITE --dir dags -f --nocleanup | tee log-plan-${TOP_DAX}-${REMOTE_SITE}
	if [ $? != 0 ]; then
        	echo "Failed to plan workflow."
        	exit 1
	fi
fi

# Update run state, use the remote site with "_noglideins" stripped off as the host name
REMOTE_PART=${REMOTE_SITE%%_*}
while read LINE ; do
    RUN_ID=`echo $LINE | awk '{print $1}'`
    /home/scec-02/cybershk/runs/runmanager/edit_run.py ${RUN_ID} "Status=Initial" "SGT_Host=${REMOTE_PART}" "Comment=Planned SGT DAX, assigned sgt host" "Job_ID=NULL"
    if [ $? != 0 ]; then
	echo "Unable to update Status, SGT_Host, Comment, Job_ID for run ${RUN_ID}"
	exit 1
    fi
done < ${RUN_FILE}

