#!/bin/sh

if [ $# -lt 2 ]; then
	echo "Usage:  $0 < <integrated dir> | <site run_ID> > remote_site"
	echo "Example: $0  1234543210_Integrated_dax bluewaters"
	echo "Example: $0  USC 2494 stampede"
	exit 1
fi

if [ $# -eq 2 ]; then
	DAX_DIR=$1
	REMOTE_SITE=$2
else
	RUN_ID=$2
	DAX_DIR=${1}_Integrated_dax/run_${RUN_ID}
	REMOTE_SITE=$3
fi

cd $DAX_DIR
TOP_DAX=""
TWO_LEVELS=0
TIMESTAMP=""
pwd
cat sites.list
cp sites.list sites.list.debug

#determine if we have 1 site or many
if [ `cat sites.list | wc -l` -gt 1 ]; then
	#more than 1 site
	TIMESTAMP=${DAX_DIR%%_*}
	TOP_DAX=CyberShake_Integrated_${TIMESTAMP}.dax
	TWO_LEVELS=1
	echo "Multiple sites."
else
	SITE=${DAX_DIR%%_*}
	TOP_DAX=CyberShake_Integrated_${SITE}.dax
	echo "Single site."
fi

#if we use HPC, we need to convert RLS parameters
#host_count = host_xcount
#count = xcount * host_xcount
#if [ $REMOTE_SITE == "hpc" ]; then
#	sed -i 's/xcount/count/g' $DAX
#fi


# Verify the run table
if [ "$TWO_LEVELS" -eq 1 ]; then
	RUN_FILE=run_table.txt
else
	RUN_FILE=../run_table.txt
fi

if [ ! -e ${RUN_FILE} ]; then
    echo "${RUN_FILE} not found in plan_full.sh, aborting."
    exit 1
else
    # Verify existing IDs are valid
    echo "Using existing run_ids from ${RUN_FILE}"

    if [ "$TWO_LEVELS" -eq 0 ]; then
	#Just look for the 1 site you need
	FOUND=0
        while read LINE ; do
        	FILE_RUN_ID=`echo $LINE | awk '{print $1}'`
        	SITE_NAME=`echo $LINE | awk '{print $2}'`
		if [ "$FILE_RUN_ID" -eq "$RUN_ID" ]; then
		        /home/scec-02/cybershk/runs/runmanager/valid_run.py ${RUN_ID} ${SITE_NAME} SGT_PLAN
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
      else
	#Use all the entries
	while read LINE; do
		RUN_ID=`echo $LINE | awk '{print $1}'`
                SITE_NAME=`echo $LINE | awk '{print $2}'`
                /home/scec-02/cybershk/runs/runmanager/valid_run.py ${RUN_ID} ${SITE_NAME} SGT_PLAN
                if [ $? != 0 ]; then
                	echo "Run ${RUN_ID} not in expected state"
                        exit 1
                fi
        done < ${RUN_FILE}
      fi
fi

# Remove any old plan logs
rm log-plan-${TOP_DAX}-*

# Modify properties file if 2 levels
propfile=/home/scec-02/cybershk/runs/config/properties.full

if [ "$REMOTE_SITE" == "bluewaters" ]; then
	OLD_PROXY=`echo $X509_USER_PROXY`
	export X509_USER_PROXY=x509up_u33527
	propfile=/home/scec-02/cybershk/runs/config/properties.bluewaters
fi

if [ $TWO_LEVELS -eq 1 ]; then
	cp $propfile properties.full.top
#	echo "pegasus.dir.storage=data/SgtFiles/${TIMESTAMP}" >> properties.full.top
#	echo "pegasus.dir.storage=data/PPFiles/${TIMESTAMP}" >> properties.full.top
	echo "pegasus.dagman.maxjobs=15" >> properties.full.top
	newpropfile=`pwd`/properties.full.top
	echo "pegasus-plan --conf=${newpropfile} --dax $TOP_DAX --rescue 100 -s $REMOTE_SITE,shock,local --output-site ${REMOTE_SITE} --dir dags -f --nocleanup | tee log-plan-${TOP_DAX}-${REMOTE_SITE}"
	pegasus-plan --conf=${newpropfile} --dax $TOP_DAX --rescue 100 -s $REMOTE_SITE,shock,local -o $REMOTE_SITE --dir dags -f --nocleanup | tee log-plan-${TOP_DAX}-${REMOTE_SITE}
	if [ $? != 0 ]; then
	    echo "Failed to plan workflow."
	    exit 1
	fi
else
	cp $propfile properties.full.onesite
	newpropfile=`pwd`/properties.full.onesite
#        echo "pegasus.dir.storage=data/SgtFiles/${SITE}" >> properties.full.onesite
#	echo "pegasus.dir.storage=data/PPFiles/${SITE}/${RUN_ID}" >> properties.full.onesite
	echo "pegasus-plan --conf=${newpropfile} --dax $TOP_DAX --rescue 100 -s $REMOTE_SITE,shock,local --output-site ${REMOTE_SITE} --output-site bluewaters --dir dags -f --nocleanup | tee log-plan-${TOP_DAX}-${REMOTE_SITE}"
        pegasus-plan --conf=${newpropfile} --dax $TOP_DAX --rescue 100 -s $REMOTE_SITE,shock,local -o $REMOTE_SITE -o bluewaters --dir dags -f --nocleanup | tee log-plan-${TOP_DAX}-${REMOTE_SITE}
	if [ $? != 0 ]; then
        	echo "Failed to plan workflow."
        	exit 1
	fi
fi

#Grab catalogs, put in metadata.local file
#TC
LINE=`grep pegasus.catalog.transformation.file $newpropfile`
TC_PATH=${LINE#*=}
EXEC_DIR=`grep pegasus-run log-plan-${TOP_DAX}-${REMOTE_SITE} | cut -d" " -f3`
echo "Site $REMOTE_SITE" > ${EXEC_DIR}/metadata.local
echo "TC $TC_PATH" >> ${EXEC_DIR}/metadata.local
more $TC_PATH >> ${EXEC_DIR}/metadata.local
#Site
LINE=`grep pegasus.catalog.site.file $newpropfile`
SC_PATH=${LINE#*=}
echo "SC $SC_PATH" >> ${EXEC_DIR}/metadata.local
more $SC_PATH >> ${EXEC_DIR}/metadata.local
#RC location
LINE=`grep pegasus.catalog.replica.db.url $newpropfile`
RC_PATH=${LINE#*=}
echo "RC location $RC_PATH" >> ${EXEC_DIR}/metadata.local

# Update run state, use the remote site with "_noglideins" stripped off as the host name
REMOTE_PART=${REMOTE_SITE%%_*}
if [ "$TWO_LEVELS" -eq 1 ]; then
	while read LINE ; do
	    RUN_ID=`echo $LINE | awk '{print $1}'`
	    /home/scec-02/cybershk/runs/runmanager/edit_run.py ${RUN_ID} "Status=Initial" "SGT_Host=${REMOTE_PART}" "Comment=Planned SGT DAX, assigned sgt host" "Job_ID=NULL" "PP_Host=${REMOTE_PART}"
	    if [ $? != 0 ]; then
		echo "Unable to update Status, SGT_Host, Comment, Job_ID for run ${RUN_ID}"
		exit 1
	    fi
	done < ${RUN_FILE}
else
	/home/scec-02/cybershk/runs/runmanager/edit_run.py ${RUN_ID} "Status=Initial" "SGT_Host=${REMOTE_PART}" "Comment=Planned SGT DAX, assigned sgt host" "Job_ID=NULL" "PP_Host=${REMOTE_PART}"
        if [ $? != 0 ]; then
                echo "Unable to update Status, SGT_Host, Comment, Job_ID for run ${RUN_ID}"
                exit 1
        fi
fi

if [ "$REMOTE_SITE" == "bluewaters" ]; then
	export X509_USER_PROXY=$OLD_PROXY
fi
