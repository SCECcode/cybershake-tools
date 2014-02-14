#!/bin/bash

if [ $# -lt 4 ]; then
        echo "Usage: $0 < -v4 | -vh > <erf> <rup_var> <sgt var> < <site 1> [site 2] ... [site N] | -f <file with sites> | -r <run_table file> >"
        echo "Example: $0 -v4 35 3 5 USC FIL PAS"
	echo "Example: $0 -vh 35 4 7 -f sites.file"
        echo "Example: $0 -vh 35 3 5 -r run_table.txt"
	exit 1
fi

ALL_ARGS=$@
ERF=$2
RUP_VAR=$3
SGT_VAR=$4
VEL_MODEL="1"
VEL_STR=$1
if [ $1 == "-v4" ]; then
	VEL_MODEL="1"
elif [ $1 == "-vh" ]; then
	VEL_MODEL="2"
else
	echo "$1 is not a recognized velocity model (-v4 or -vh)"
	exit 2
fi

#Assume more than 1 site
#If 1 site, we use a different RUN_DIR and DAX_FILE later
TIMESTAMP=`date +%s`
RUN_DIR=${TIMESTAMP}_SGT_dax
DAX_FILE=CyberShake_SGT_${TIMESTAMP}.dax

if [ $5 == "-f" ]; then #using file with site names
	if [ $# -ne 6 ]; then
		echo "Usage: $0 <-v4 | -vh> <erf> <rup_var> <sgt var> < <site 1> [site 2] ... [site N] | -f <file with sites> | -r <run_table file> >"
        	echo "Example: $0 -v4 35 3 5 USC FIL PAS"
	        echo "Example: $0 -vh 35 4 7 -f sites.file"
	        echo "Example: $0 -vh 35 4 5 -r run_table.txt"
		exit 2
	fi
	FILE=$6
	SITE_STRING=""
	let x=0
	for site in `more $FILE`; do
		SITE_STRING="$SITE_STRING-$site"
		SITES[$x]=$site
		x=$(($x+1))
	done
	mkdir $RUN_DIR
	cp $FILE ${RUN_DIR}/sites.list
elif [ $5 == "-r" ]; then #using run table file
	if [ $# -ne 7 ]; then
                echo "Usage: $0 <-v4 | -vh> <erf> <rup_var> <sgt var> < <site 1> [site 2] ... [site N] | -f <file with sites> | -r <run_table file> >"
                echo "Example: $0 -v4 35 4 5 USC FIL PAS"
                echo "Example: $0 -vh 35 3 7 -f sites.file"
                echo "Example: $0 -vh 35 3 5 -r run_table.txt"
                exit 2
        fi
	FILE=$6
	SITE_STRING=""
	let x=0
	for site in `more $FILE | cut -d " " -f2`; do
		SITE_STRING="$SITE_STRING-$site"
		SITES[$x]=$site
		x=$(($x+1))
	done
	mkdir $RUN_DIR
	cp $FILE ${RUN_DIR}/run_table.txt
else
	# Construct site-string and array of site names
	SITE_STRING=$5
	SITES[0]=$5
	echo "$5" > /tmp/sites.list
	let x=1
	shift 5
	while (( "$#" )); do
		SITE_STRING="$SITE_STRING-$1"
		SITES[$x]=$1
		echo $1 >> /tmp/sites.list
		shift
		x=$(($x+1))
	done
	if [ ${#SITES[@]} -gt 1 ]; then
		#Use timestamp directory and file
		mkdir $RUN_DIR
		mv /tmp/sites.list ${RUN_DIR}/sites.list
	else
		#One site, use site name
		RUN_DIR=${SITES[0]}_SGT_dax
		mkdir $RUN_DIR
		mv /tmp/sites.list ${RUN_DIR}/sites.list
		DAX_FILE=CyberShake_SGT_${SITES[0]}.dax
	fi
fi

echo "run dir: $RUN_DIR"
RUN_FILE=${RUN_DIR}/run_table.txt
RUN_ID_STRING=""
if [ ! -e ${RUN_FILE} ]; then
    # Create new run ids
    for SITE in ${SITES[@]}; do
	echo "Creating run for site ${SITE}"
        # Create new run
	RUN_ID=`/home/scec-02/cybershk/runs/runmanager/create_run.py ${SITE} ${ERF} ${SGT_VAR} ${VEL_MODEL} ${RUP_VAR}`
	if [ $? -ne 0 ]; then
	    echo "Failed to create new run for ${SITE}."
	    exit 1
	fi
	echo "${RUN_ID} ${SITE}" >> ${RUN_FILE}
	if [ "${RUN_ID_STRING}" == "" ]; then
	    RUN_ID_STRING="${RUN_ID}"
	else
	    RUN_ID_STRING="${RUN_ID_STRING} ${RUN_ID}"
	fi
    done
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
	if [ "${RUN_ID_STRING}" == "" ]; then
	    RUN_ID_STRING="${RUN_ID}"
	else
	    RUN_ID_STRING="${RUN_ID_STRING} ${RUN_ID}"
	fi
    done < ${RUN_FILE}
fi


# Compile the DAX generator
DAX_GEN_DIR="dax-generator"
javac -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar:${DAX_GEN_DIR}/lib/opensha-commons-1.1.4.jar ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/CyberShake_SGT_DAXGen.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/DBConnect.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/RunIDQuery.java

if [ $? -ne 0 ]; then
	exit 1
fi

echo $DAX_FILE
# Run the DAX generator
echo "java -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar:${DAX_GEN_DIR}/lib/opensha-commons-1.1.4.jar org/scec/cme/cybershake/dax3/CyberShake_SGT_DAXGen $DAX_FILE `pwd`/${RUN_DIR} ${VEL_STR} -r ${RUN_ID_STRING}"
java -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar:${DAX_GEN_DIR}/lib/opensha-commons-1.1.4.jar org/scec/cme/cybershake/dax3/CyberShake_SGT_DAXGen $DAX_FILE `pwd`/${RUN_DIR} ${VEL_STR} -r ${RUN_ID_STRING}

if [ $? -ne 0 ]; then
	exit 1
fi


mv $DAX_FILE $RUN_DIR
let x=0
if [ ${#SITES[@]} -gt 1 ]; then
	#more than 1 site, need to move the sub-daxes into the run dir
	for SITE in ${SITES[@]}; do
		mv CyberShake_SGT_${SITE}_${x}.dax ${RUN_DIR}/CyberShake_SGT_${SITE}_${x}.dax
		x=$(($x+1))	
	done
fi


# Update status, comment on each run
while read LINE ; do
    RUN_ID=`echo $LINE | awk '{print $1}'`
    /home/scec-02/cybershk/runs/runmanager/edit_run.py ${RUN_ID} "Status=Initial" "Comment=SGT DAX created" "Job_ID=NULL" "Submit_Dir=NULL"
    if [ $? != 0 ]; then
        echo "Unable to update Status, Comment, Job_ID, Submit_Dir for run ${RUN_ID}"
	# Continue updating runs
    fi
done < ${RUN_FILE}
