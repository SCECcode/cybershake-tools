#!/bin/bash

if [ $# -lt 2 ]; then
	echo "Usage:  $0 < SGT_dir | site run_id > remote_site"
	echo "Example: $0 1234543210_SGT_dax ranger"
	echo "Example: $0 USC 1359 stampede"
	exit 1
fi

ONE_SITE=0
TOP_DAX=""
if [ $# -eq 2 ]; then
	DAX_DIR=$1
	REMOTE_SITE=$2
else
	SITE=$1
	DAX_DIR=${SITE}_SGT_dax
	RUN_ID=$2
	REMOTE_SITE=$3
	TOP_DAX=CyberShake_SGT_${SITE}.dax
	ONE_SITE=1
	
fi

if [ $REMOTE_SITE == "titan" || $REMOTE_SITE == "summit" ]; then
	echo "Using OLCF proxy."
	export X509_USER_PROXY=/tmp/x509up_u7588
fi

propfile=/home/scec-02/cybershk/runs/config/properties.sgt

#To pick up the reservation parameters
if [ $REMOTE_SITE == "bluewaters" ]; then
	echo "Using reservation parameters."
	#Select either res1 or res2, cyclically
	#RES_FILE='res.txt'
	#if [ -f $RES_FILE ]; then
	#	RES_VALUE=`cat $RES_FILE`
	#	if [ $RES_VALUE -eq 0 ]; then
	#		#Use baln1
	#		propfile=/home/scec-02/cybershk/runs/config/properties.sgt.baln1
	#		echo 1 > $RES_FILE
	#	else
	#		#Use baln2
	#		/home/scec-02/cybershk/runs/config/properties.sgt.baln2
	#		echo 0 > $RES_FILE
	#	fi
	#else
	#	#No res file yet
	#	#Use baln2
		propfile=/home/scec-02/cybershk/runs/config/properties.sgt.baln2
        #        echo 1 > $RES_FILE
	#fi
fi

cd $DAX_DIR
TWO_LEVELS=0
TIMESTAMP=""

#determine if we have 1 site or many, if we specified a directory
if [ $ONE_SITE -ne 1 ]; then
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
		ONE_SITE=1
	fi
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
	
    if [ "$ONE_SITE" -eq 0 ]; then
	#iterate through all of the entries in the run table
	while read LINE; do
		RUN_ID=`echo $LINE | awk '{print $1}'`
                SITE_NAME=`echo $LINE | awk '{print $2}'`
		/home/scec-02/cybershk/runs/runmanager/valid_run.py ${RUN_ID} ${SITE_NAME} SGT_PLAN
		if [ $? != 0 ]; then
                	echo "Run ${RUN_ID} not in expected state"
                        exit 1
                fi
	done < ${RUN_FILE}
    else
	#just look for the one you need
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
    fi
fi

# Remove any old plan logs
rm log-plan-${TOP_DAX}-*

if [ "$ONE_SITE" -eq 1 ]; then
	cd run_${RUN_ID}
fi

OUTPUT_SITE=$REMOTE_SITE

# Modify properties file if 2 levels
if [ $TWO_LEVELS -eq 1 ]; then
	cp $propfile properties.sgt.top
	#pegasus.dir.storage replaced by --output-dir
	#echo "pegasus.dir.storage=data/SgtFiles/${TIMESTAMP}" >> properties.sgt.top
	echo "pegasus.dagman.maxjobs=15" >> properties.sgt.top
	newpropfile=`pwd`/properties.sgt.top
	echo "pegasus-plan --conf=${newpropfile} --dax $TOP_DAX --rescue 100 -s $REMOTE_SITE,shock -o ${REMOTE_SITE} --dir dags -f --nocleanup --output-dir data/SgtFiles/${TIMESTAMP} | tee log-plan-${TOP_DAX}-${REMOTE_SITE}"
	pegasus-plan --conf=${newpropfile} --dax $TOP_DAX --rescue 100 -s $REMOTE_SITE,shock -o $REMOTE_SITE --dir dags -f --nocleanup --output-dir data/SgtFiles/${TIMESTAMP} | tee log-plan-${TOP_DAX}-${REMOTE_SITE}
	if [ $? != 0 ]; then
	    echo "Failed to plan workflow."
	    exit 1
	fi
else
	cp $propfile properties.sgt.onesite
	#pegasus.dir.storage replaced by --output-dir
        #echo "pegasus.dir.storage=data/SgtFiles/${SITE}" >> properties.sgt.onesite
	if [ $REMOTE_SITE == "titan" || $REMOTE_SITE == "summit" ]; then
	        sed -i 's/u801878/u7588/g' properties.sgt.onesite
		#Add titan-pilot to remote site list so we can use pilot jobs for SGTs
		#REMOTE_SITE=$REMOTE_SITE,titan-pilot
	fi
	echo "pegasus-plan -vv --conf=${propfile} --dax $TOP_DAX --rescue 100 -s $REMOTE_SITE,shock -o ${OUTPUT_SITE} --dir dags -f --nocleanup --output-dir data/SgtFiles/${SITE} | tee log-plan-${TOP_DAX}-${REMOTE_SITE}"
	echo $PATH
        pegasus-plan -vv --conf=properties.sgt.onesite --dax $TOP_DAX --rescue 100 -s $REMOTE_SITE,shock -o $OUTPUT_SITE --dir dags -f --nocleanup --output-dir data/SgtFiles/${SITE} | tee log-plan-${TOP_DAX}-${REMOTE_SITE}
	if [ $? != 0 ]; then
        	echo "Failed to plan workflow."
        	exit 1
	fi
fi

# Update run state, use the remote site with "_noglideins" stripped off as the host name
# Use output site, it can't have titan-pilot added to it
REMOTE_PART=${OUTPUT_SITE%%_*}

if [ "$ONE_SITE" -eq 0 ]; then
	while read LINE ; do
	    RUN_ID=`echo $LINE | awk '{print $1}'`
	    /home/scec-02/cybershk/runs/runmanager/edit_run.py ${RUN_ID} "Status=Initial" "SGT_Host=${REMOTE_PART}" "Comment=Planned SGT DAX, assigned sgt host" "Job_ID=NULL"
	    if [ $? != 0 ]; then
		echo "Unable to update Status, SGT_Host, Comment, Job_ID for run ${RUN_ID}"
		exit 1
	    fi
	done < ${RUN_FILE}
else
	#We already know the 1 run ID
	/home/scec-02/cybershk/runs/runmanager/edit_run.py ${RUN_ID} "Status=Initial" "SGT_Host=${REMOTE_PART}" "Comment=Planned SGT DAX, assigned sgt host" "Job_ID=NULL"
	if [ $? != 0 ]; then
                echo "Unable to update Status, SGT_Host, Comment, Job_ID for run ${RUN_ID}"
                exit 1
        fi
fi
