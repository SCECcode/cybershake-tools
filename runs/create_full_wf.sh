#!/bin/bash

show_help() {
	ROOT_DIR=`dirname $0`
	DAX_GEN_DIR="${ROOT_DIR}/dax-generator"
	javac -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar:${DAX_GEN_DIR}/lib/opensha-commons-1.1.4.jar ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/CyberShake_Integrated_DAXGen.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/DBConnect.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/RunIDQuery.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/CyberShake_SGT_DAXGen.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/SGT_DAXParameters.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/CyberShake_PP_DAXGen.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/PP_DAXParameters.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/CyberShake_Workflow_Container.java
	JAVA_OUT=`java -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar:${DAX_GEN_DIR}/lib/opensha-commons-1.1.4.jar org/scec/cme/cybershake/dax3/CyberShake_Integrated_DAXGen -h`
	cat << EOF
	Usage: $0 [-h] <-v VELOCITY_MODEL> <-e ERF_ID> <-v RUP_VAR_ID> <-g SGT_ID> <-f FREQ> <-s SITE> [-q SRC_FREQ] [--sgtargs <SGT dax generator args>] [--ppargs <post-processing args>]
		-h			display this help and exit
                -v VELOCITY_MODEL       select velocity model, one of v4 (CVM-S4), vh (CVM-H), vsi (CVM-S4.26), vs1 (SCEC 1D model), vhng (CVM-H, no GTL), or vbbp (BBP 1D model).
                -e ERF_ID               ERF ID
                -r RUP_VAR_ID           Rupture Variation ID
                -g SGT_ID               SGT ID
                -f FREQ                 Simulation frequency (0.5 or 1.0 supported)
		-s SITE                 Site short name
                -q SRC_FREQ             Optional: SGT source filter frequency

	Can be followed by optional arguments:
	${JAVA_OUT}
EOF
}


if [ $# -lt 1 ]; then
	show_help
	exit 0
fi

getopts_args=""
for i in $@; do
        if [ "$i" == "--sgtargs" -o "$i" == "--ppargs" ]; then
                break
        else
                getopts_args="$getopts_args $i"
        fi
done
echo $getopts_args

OPTIND=1
VEL_STR=""
ERF=""
RUP_VAR=""
SGT_VAR=""
FREQ=""
SRC_FREQ=""
SITE=""
while getopts ":h:v:e:r:g:f:q:s:" opt $getopts_args; do
        case $opt in
                h)      show_help
                        exit 0
                        ;;
                v)      VEL_STR=$OPTARG
                        ;;
                e)      ERF=$OPTARG
                        ;;
                r)      RUP_VAR=$OPTARG
                        ;;
                g)      SGT_VAR=$OPTARG
                        ;;
                f)      FREQ=$OPTARG
                        if [ "$SRC_FREQ" == "" ]; then
                                SRC_FREQ=$FREQ
                        fi
                        ;;
                q)      SRC_FREQ=$OPTARG
                        ;;
                s)      SITE=$OPTARG
                        ;;
                *)      break
        esac
done
shift "$((OPTIND-1))"

if [ "$VEL_STR" == "" ]; then
        echo "Must specify velocity model."
        exit 1
fi
if [ "$ERF" == "" ]; then
        echo "Must specify ERF ID."
        exit 1
fi
if [ "$RUP_VAR" == "" ]; then
        echo "Must specify rupture variation ID."
        exit 1
fi
if [ "$SGT_VAR" == "" ]; then
        echo "Must specify SGT variation ID."
        exit 1
fi
if [ "$FREQ" == "" ]; then
        echo "Must specify frequency."
        exit 1
fi
if [ "$SITE" == "" ]; then
        echo "Must specify site."
        exit 1
fi

ALL_ARGS=$@
#ERF=$2
#RUP_VAR=$3
#SGT_VAR=$4
#FREQ=$5
VEL_ID="1"
#VEL_STR=$1
if [ $VEL_STR == "v4" ]; then
	VEL_ID="1"
elif [ $VEL_STR == "vh" ]; then
	#4 for 11.9
	VEL_ID="4"
elif [ $VEL_STR == "vsi" ]; then
	#CVM-SI4.26
	VEL_ID="5"
elif [ $VEL_STR == "vs1" ]; then
	#SCEC 1D
	VEL_ID="6"
elif [ $VEL_STR == "vhng" ]; then
	#CVM-H, no GTL
	VEL_ID="7"
elif [ $VEL_STR == "vbbp" ]; then
	#BBP 1D (Northridge)
	VEL_ID="8"
else
	echo "$1 is not a recognized velocity model."
	exit 2
fi

#Assume more than 1 site
#If 1 site, we use a different RUN_DIR and DAX_FILE later
#TIMESTAMP=`date +%s`
#RUN_DIR=${TIMESTAMP}_Integrated_dax
#DAX_FILE=CyberShake_Integrated_${TIMESTAMP}.dax

#if [ $6 == "-f" ]; then #using file with site names
#	if [ $# -lt 8 ]; then
#		echo "$USAGE_STRING"
#		exit 2
#	fi
#	FILE=$7
#	SITE_STRING=""
#	let x=0
#	for site in `cat $FILE`; do
#		SITE_STRING="$SITE_STRING-$site"
#		SITES[$x]=$site
#		x=$(($x+1))
#	done
#	mkdir $RUN_DIR
#	cp $FILE ${RUN_DIR}/sites.list
#	shift 7
#elif [ $6 == "-r" ]; then #using run table file
#	if [ $# -lt 8 ]; then
#		echo "$USAGE_STRING"
#		echo "$#"
#                exit 3
#        fi
#	FILE=$7
#	SITE_STRING=""
#	let x=0
#	for site in `cat $FILE | cut -d " " -f2`; do
#		SITE_STRING="$SITE_STRING-$site"
#		SITES[$x]=$site
#		echo $site >> ${RUN_DIR}/sites.list
#		x=$(($x+1))
#	done
#	mkdir $RUN_DIR
#	cp $FILE ${RUN_DIR}/run_table.txt
#	shift 7
#else
	# Construct site-string and array of site names
	# Assume single site
	#SITE_STRING=$6
	#SITES[0]=$6
	#echo "$6" > /tmp/sites.list
	#let x=1
	#shift 6
	#while [[ $# -gt 0 && "$1" != "--ppargs" && "$1" != "--sgtargs" ]]; do
	#	SITE_STRING="$SITE_STRING-$1"
	#	SITES[$x]=$1
	#	echo $1 >> /tmp/sites.list
	#	shift
	#	x=$(($x+1))
	#done
	SITE_STRING=$SITE
	SITES[0]=$SITE
	echo "$SITE" > /tmp/sites.list
	if [ ${#SITES[@]} -gt 1 ]; then
		#Use timestamp directory and file
		mkdir $RUN_DIR
		mv /tmp/sites.list ${RUN_DIR}/sites.list
	else
		#One site, use site name
		RUN_DIR=${SITES[0]}_Integrated_dax
		mkdir $RUN_DIR
		mv /tmp/sites.list ${RUN_DIR}/sites.list
		DAX_FILE=CyberShake_Integrated_${SITES[0]}.dax
	fi
#fi

#PPARGS=""
#if [ "$1" == "--ppargs" ]; then
#	while (( "$#" )); do
#		PPARGS="${PPARGS} $1"
#		shift 1
#	done
#fi

echo "run dir: $RUN_DIR"
RUN_FILE=${RUN_DIR}/run_table.txt

RUN_ID_STRING=""
if [ ! -e ${RUN_FILE} ]; then
    # Create new run ids
    for SITE in ${SITES[@]}; do
	echo "Creating run for site ${SITE}"
        # Create new run
	RUN_ID=`/home/scec-02/cybershk/runs/runmanager/create_run.py ${SITE} ${ERF} ${SGT_VAR} ${VEL_ID} ${RUP_VAR} ${FREQ} ${SRC_FREQ}`
	if [ $? -ne 0 ]; then
	    echo "Failed to create new run for ${SITE}."
	    exit 1
	fi
	echo "${RUN_ID} ${SITE} ${ERF} ${SGT_VAR} ${RUP_VAR} ${VEL_ID} ${FREQ} ${SRC_FREQ}" >> ${RUN_FILE}
	if [ "${RUN_ID_STRING}" == "" ]; then
	    RUN_ID_STRING="${RUN_ID}"
	else
	    RUN_ID_STRING="${RUN_ID_STRING} ${RUN_ID}"
	fi
    done
else
    # Verify existing IDs are valid
    echo "Using existing run_ids from ${RUN_FILE}"

    for SITE in ${SITES[@]}; do
	echo "Searching for run for site ${SITE}"

	while read LINE ; do
		FOUND=0
		RUN_ID=`echo $LINE | awk '{print $1}'`
		SITE_NAME=`echo $LINE | awk '{print $2}'`
        	RF_ERF_ID=`echo $LINE | awk '{print $3}'`
        	RF_SGT_VAR=`echo $LINE | awk '{print $4}'`
        	RF_RUP_VAR=`echo $LINE | awk '{print $5}'`
        	RF_VEL_ID=`echo $LINE | awk '{print $6}'`
		RF_FREQ=`echo $LINE | awk '{print $7}'`
		RF_SRC_FREQ=$RF_FREQ
                if [[ `echo $LINE | awk '{print NF}'` -eq 8 ]]; then
                        #Then we've specificed a different source frequency
                        RF_SRC_FREQ=`echo $LINE | awk '{print $8}'`
                fi
        	#First, see if this is a old format run_ID file
        	if [[ "$RF_ERF_ID" -eq "" ]]; then
        	        #This is not what we're looking for
        	        continue
        	fi
		if [[ "$SITE" != "$SITE_NAME" ]]; then
			continue
		fi
        	#See if this run could plausibly be the one we want
        	if [[ "$RF_ERF_ID" -ne "" && "$RF_ERF_ID" -ne "$ERF" ]]; then
        	        continue
        	fi
        	if [[ "$RF_SGT_VAR" -ne "" && "$RF_SGT_VAR" -ne "$SGT_VAR" ]]; then
        	        continue
        	fi
        	if [[ "$RF_RUP_VAR" -ne "" && "$RF_RUP_VAR" -ne "$RUP_VAR" ]]; then
        	        continue
        	fi
        	if [[ "$RF_VEL_ID" -ne "" && "$RF_VEL_ID" -ne "$VEL_ID" ]]; then
                	continue
        	fi
		if [[ "$RF_FREQ" != "" && "$RF_FREQ" != "$FREQ" ]]; then
			continue
		fi
		echo "/home/scec-02/cybershk/runs/runmanager/valid_run.py ${RUN_ID} ${SITE_NAME} SGT_PLAN"
		/home/scec-02/cybershk/runs/runmanager/valid_run.py ${RUN_ID} ${SITE_NAME} SGT_PLAN
		if [ $? != 0 ]; then
        	    echo "Run ${RUN_ID} not in expected state"
		    #Don't create new run here; keep checking in the run file
		else
		    FOUND=1
		    RUN_ID_STRING="${RUN_ID_STRING} ${RUN_ID}"
		    break
		fi
	done < ${RUN_FILE}
	if [ $FOUND -eq 0 ]; then
		#need to create new run
		echo "${SITE} ${ERF} ${SGT_VAR} ${VEL_ID} ${RUP_VAR} ${FREQ}"
		RUN_ID=`/home/scec-02/cybershk/runs/runmanager/create_run.py ${SITE} ${ERF} ${SGT_VAR} ${VEL_ID} ${RUP_VAR} ${FREQ} ${SRC_FREQ}`
                if [ $? -ne 0 ]; then
                	echo "Failed to create run."
                	exit 2
                fi
                echo "${RUN_ID} ${SITE} ${ERF} ${SGT_VAR} ${RUP_VAR} ${VEL_ID} ${FREQ} ${SRC_FREQ}" >> ${RUN_FILE}
		RUN_ID_STRING="${RUN_ID_STRING} ${RUN_ID}"
	fi
    done
fi



if [ ${#SITES[@]} -eq 1 ]; then
	echo "RUN_ID is ${RUN_ID}"
        RUN_DIR=${RUN_DIR}/run_${RUN_ID}
        mkdir -p $RUN_DIR
fi

# Compile the DAX generator
DAX_GEN_DIR="dax-generator"
javac -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar:${DAX_GEN_DIR}/lib/opensha-commons-1.1.4.jar ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/CyberShake_Integrated_DAXGen.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/DBConnect.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/RunIDQuery.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/CyberShake_SGT_DAXGen.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/SGT_DAXParameters.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/CyberShake_PP_DAXGen.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/PP_DAXParameters.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/CyberShake_Workflow_Container.java

if [ $? -ne 0 ]; then
	exit 1
fi

echo $DAX_FILE
# Run the DAX generator
echo "java -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar:${DAX_GEN_DIR}/lib/opensha-commons-1.1.4.jar org/scec/cme/cybershake/dax3/CyberShake_Integrated_DAXGen $DAX_FILE `pwd`/${RUN_DIR} ${VEL_STR} $@ -rl ${RUN_ID_STRING}"
java -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar:${DAX_GEN_DIR}/lib/opensha-commons-1.1.4.jar org/scec/cme/cybershake/dax3/CyberShake_Integrated_DAXGen $DAX_FILE `pwd`/${RUN_DIR} $@ -rl ${RUN_ID_STRING}

if [ $? -ne 0 ]; then
	exit 1
fi

let x=0
if [ ${#SITES[@]} -gt 1 ]; then
	mv $DAX_FILE $RUN_DIR
	#more than 1 site, need to move the sub-daxes into the run dir
	for SITE in ${SITES[@]}; do
		mv CyberShake_SGT_${SITE}_${x}.dax ${RUN_DIR}/CyberShake_SGT_${SITE}_${x}.dax
		mv CyberShake_${SITE}*.dax ${RUN_DIR}
		x=$(($x+1))	
	done
else
	#only 1 site, need to move the daxes into the run_$RUN_ID dir
	mv CyberShake_Integrated_${SITE}.dax $RUN_DIR
	mv CyberShake_SGT_${SITE}*.dax ${RUN_DIR}
	mv CyberShake_${SITE}*.dax ${RUN_DIR}
	mv ${SITE}_Integrated_dax/sites.list ${RUN_DIR}/
fi


# Update status, comment on each run
for RUN_ID in $RUN_ID_STRING; do
    /home/scec-02/cybershk/runs/runmanager/edit_run.py ${RUN_ID} "Status=Initial" "Comment=SGT DAX created" "Job_ID=NULL" "Submit_Dir=NULL"
    if [ $? != 0 ]; then
        echo "Unable to update Status, Comment, Job_ID, Submit_Dir for run ${RUN_ID}"
        # Continue updating runs
    fi
done
