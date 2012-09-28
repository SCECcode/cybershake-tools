#!/bin/bash

if [ $# -lt 5 ]; then
        echo "Usage: $0 <site> <-v4 | -vh | -vhs > <erf> <rup_var> <sgt_var> [-p <partitions>] [-r] [-f <replication factor>] [-s] [-noinsert] [-cj | -ms | -cs] [-hf [hf cutoff]] [-mh] [-mb] [-jbmem] [-hfmem] [-sql]"
        echo "Example: $0 USC -v4 35 3 5 -p 80"
        exit 1
fi

SITE=$1
VEL=$2
VEL_ID=0
ERF=$3
RUP_VAR=$4
SGT_VAR=$5

shift 5
OPT_ARGS=$@

if [ $VEL == -v4 ]; then
	VEL_ID=1
elif [ $VEL == -vh ]; then
	VEL_ID=2
elif [ $VEL == -vhs ]; then
	VEL_ID=3
else
	echo "Velocity option $VEL needs to be one of -v4 or -vh."
	exit 1
fi

AWP=0
for i in $OPT_ARGS; do
        if [ $i == "-awp" ]; then
                AWP=1
                break
        fi
done


#if [ $# -ge 6 ]; then
#    NUM_DAX=$6
#else
#    NUM_DAX=80
#    echo "Defaulting to ${NUM_DAX} DAXs"
#fi

#PRIORITY=""
#if [ $# -eq 7 ]; then
#    PRIORITY=$7
#fi

mkdir ${SITE}_PP_dax

# Get best-match runid from DB
RUN_FILE=${SITE}_PP_dax/run_table.txt
RUN_ID_STRING=""
if [ ! -e ${RUN_FILE} ]; then
    echo "find_run.py ${SITE} ${ERF} ${SGT_VAR} ${RUP_VAR} ${VEL_ID} ${AWP}"
    RUN_ID=`/home/scec-02/cybershk/runs/runmanager/find_run.py ${SITE} ${ERF} ${SGT_VAR} ${RUP_VAR} ${VEL_ID} ${AWP}`
    if [ $? -ne 0 ]; then
	echo "Failed to find matching run."
	exit 1
    fi
    echo "${RUN_ID} ${SITE}" >> ${RUN_FILE}
    RUN_ID_STRING=${RUN_ID}
else
    # Verify existing IDs are valid
    echo "Using existing run_ids from ${RUN_FILE}"

    while read LINE ; do
        RUN_ID=`echo $LINE | awk '{print $1}'`
        SITE_NAME=`echo $LINE | awk '{print $2}'`
	echo "/home/scec-02/cybershk/runs/runmanager/valid_run.py ${RUN_ID} ${SITE_NAME} PP_PLAN"
        /home/scec-02/cybershk/runs/runmanager/valid_run.py ${RUN_ID} ${SITE_NAME} PP_PLAN
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
javac -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/lib/sqlitejdbc-v056.jar:${DAX_GEN_DIR}/lib/opensha-commons-1.1.4.jar:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/CyberShake_PP_DAXGen.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/CyberShake_DB_DAXGen.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/DBConnect.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/PP_DAXParameters.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/RunIDQuery.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/RuptureVariationDB.java

if [ $? -ne 0 ]; then
	exit 1
fi


# Run the DAX generator
echo "java -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/lib/sqlitejdbc-v056.jar:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar org/scec/cme/cybershake/dax/CyberShake_PP_DAXGen ${RUN_ID_STRING} `pwd`/${SITE}_PP_dax ${OPT_ARGS}"
java -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/lib/sqlitejdbc-v056.jar:${DAX_GEN_DIR}/lib/opensha-commons-1.1.4.jar:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar org/scec/cme/cybershake/dax3/CyberShake_PP_DAXGen ${RUN_ID_STRING} `pwd`/${SITE}_PP_dax ${OPT_ARGS}


if [ $? -ne 0 ]; then
	exit 1
fi

mv CyberShake_${SITE}*.dax ${SITE}_PP_dax/
mv ${SITE}.db ${SITE}_PP_dax/
#mv CyberShake_${SITE}.pdax ${SITE}_PP_dax/


# Update comment, for each run
while read LINE ; do
    RUN_ID=`echo $LINE | awk '{print $1}'`
    /home/scec-02/cybershk/runs/runmanager/edit_run.py ${RUN_ID} "Comment=PP DAX created"
    if [ $? != 0 ]; then
        echo "Unable to update comment for run ${RUN_ID}"
        # Continue updating runs
    fi
done < ${RUN_FILE}
