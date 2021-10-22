#!/bin/bash

show_help() {
	DAX_GEN_DIR="dax-generator"
        javac -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/lib/sqlitejdbc-v056.jar:${DAX_GEN_DIR}/lib/opensha-commons-1.1.4.jar:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/CyberShake_Stochastic_DAXGen.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/CyberShake_DB_DAXGen.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/DBConnect.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/Stochastic_DAXParameters.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/RunIDQuery.java
        JAVA_OUT=`java -Xmx8192m -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/lib/sqlitejdbc-v056.jar:${DAX_GEN_DIR}/lib/opensha-commons-1.1.4.jar:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar org/scec/cme/cybershake/dax3/CyberShake_Stochastic_DAXGen --help`
        cat << EOF
	Usage: $0 [-h] <-l LF_RUN_ID> <-e ERF_ID> <-r RV_ID> <-f MERGE_FREQ> <-s SITE> [--daxargs DAX generator args]
                -h                      display this help and exit
		-l LF_RUN_ID		Low-frequency run ID to combine
                -e ERF_ID               ERF ID
                -r RUP_VAR_ID           Rupture Variation ID
                -f MERGE_FREQ           Frequency to merge LF and stochastic results
                -s SITE                 Site short name

        Can be followed by DAX arguments:
        ${JAVA_OUT}
EOF
}

if [ $# -lt 1 ]; then
        show_help
        exit 0
fi

getopts_args=""
for i in $@; do
        if [ "$i" == "--daxargs" ]; then
                break
        else
                getopts_args="$getopts_args $i"
        fi
done
echo $getopts_args

OPTIND=1
LF_ID=""
ERF=""
RUP_VAR=""
MERGE_FREQ=""
SITE=""
while getopts ":hl:e:r:f:s:" opt $getopts_args; do
        case $opt in
                h)      show_help
                        exit 0
                        ;;
		l)	LF_ID=$OPTARG
			;;
                e)      ERF=$OPTARG
                        ;;
                r)      RUP_VAR=$OPTARG
                        ;;
                f)      MERGE_FREQ=$OPTARG
                        ;;
                s)      SITE=$OPTARG
                        ;;
                *)      break
        esac
done
shift "$((OPTIND))"

if [ "$LF_ID" == "" ]; then
        echo "Must specify low-frequency ID."
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
if [ "$MERGE_FREQ" == "" ]; then
        echo "Must specify merge frequency."
        exit 1
fi
if [ "$SITE" == "" ]; then
        echo "Must specify site."
        exit 1
fi

STOCH_FREQ=10.0

OPT_ARGS=$@

mkdir ${SITE}_Stoch_dax

# Get best-match runid from DB
RUN_FILE=${SITE}_Stoch_dax/run_table.txt
RUN_ID_STRING=""
if [ ! -e ${RUN_FILE} ]; then
    echo "find_stoch_run.py ${SITE} ${LF_ID} ${ERF} ${RUP_VAR} ${MERGE_FREQ} ${STOCH_FREQ}"
    RUN_ID=`/home/scec-02/cybershk/runs/runmanager/find_stoch_run.py ${SITE} ${LF_ID} ${ERF} ${RUP_VAR} ${MERGE_FREQ} ${STOCH_FREQ}`
    if [ $? -ne 0 ]; then
        echo "Failed to find matching run, creating new run."
        RUN_ID=`/home/scec-02/cybershk/runs/runmanager/create_stoch_run.py ${SITE} ${LF_ID} ${ERF} ${RUP_VAR} ${MERGE_FREQ} ${STOCH_FREQ}`
	if [ $? -ne 0 ]; then
            echo "Failed to create new run for ${SITE}."
            exit 1
        fi
    fi
    echo "${RUN_ID} ${SITE} ${LF_ID} ${ERF} ${RUP_VAR} ${MERGE_FREQ} ${STOCH_FREQ}" >> ${RUN_FILE}
else
    # Verify existing IDs are valid
    echo "Using existing run_ids from ${RUN_FILE}"
    FOUND=0

    while read LINE ; do
        RUN_ID=`echo $LINE | awk '{print $1}'`
        SITE_NAME=`echo $LINE | awk '{print $2}'`
        RF_LF_ID=`echo $LINE | awk '{print $3}'`
        RF_ERF_ID=`echo $LINE | awk '{print $4}'`
        RF_RUP_VAR=`echo $LINE | awk '{print $5}'`
        RF_MERGE_FREQ=`echo $LINE | awk '{print $6}'`
        RF_STOCH_FREQ=`echo $LINE | awk '{print $7}'`
        #See if this run could plausibly be the one we want
        if [[ "$RF_LF_ID" -ne "" && "$RF_LF_ID" -ne "$LF_ID" ]]; then
                continue
        fi
        if [[ "$RF_ERF_ID" -ne "" && "$RF_ERF_ID" -ne "$ERF" ]]; then
                continue
        fi
        if [[ "$RF_RUP_VAR" -ne "" && "$RF_RUP_VAR" -ne "$RUP_VAR" ]]; then
                continue
        fi
        if [[ "$RF_MERGE_FREQ" != "" && "$RF_MERGE_FREQ" != "$MERGE_FREQ" ]]; then
                continue
        fi
        if [[ "$RF_STOCH_FREQ" != "" && "$RF_STOCH_FREQ" != "$STOCH_FREQ" ]]; then
                continue
        fi
        FOUND=1
	echo "/home/scec-02/cybershk/runs/runmanager/valid_run.py ${RUN_ID} ${SITE_NAME} PP_PLAN"
        /home/scec-02/cybershk/runs/runmanager/valid_run.py ${RUN_ID} ${SITE_NAME} PP_PLAN
        if [ $? != 0 ]; then
            echo "Run ${RUN_ID} not in expected state"
            exit 1
        fi
        if [[ "$FOUND" -eq "1" ]]; then
                break
        fi
    done < ${RUN_FILE}

    if [[ "${FOUND}" == 0 ]]; then
        #We didn't find a run match.  Create a new run.
        echo "create_stoch_run.py ${SITE} ${LF_ID} ${ERF} ${RUP_VAR} ${MERGE_FREQ} ${STOCH_FREQ}"
        RUN_ID=`/home/scec-02/cybershk/runs/runmanager/create_stoch_run.py ${SITE} ${LF_ID} ${ERF} ${RUP_VAR} ${MERGE_FREQ} ${STOCH_FREQ}`
        if [ $? -ne 0 ]; then
            echo "Failed to create run."
            exit 2
        fi
        echo "${RUN_ID} ${SITE} ${LF_ID} ${ERF} ${RUP_VAR} ${MERGE_FREQ} ${STOCH_FREQ}" >> ${RUN_FILE}
    fi
fi

RUN_ID_STRING=${RUN_ID}

mkdir ${SITE}_Stoch_dax/run_${RUN_ID}

# Compile the DAX generator
DAX_GEN_DIR="dax-generator"
javac -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/lib/sqlitejdbc-v056.jar:${DAX_GEN_DIR}/lib/opensha-commons-1.1.4.jar:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/CyberShake_Stochastic_DAXGen.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/CyberShake_DB_DAXGen.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/DBConnect.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/Stochastic_DAXParameters.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/RunIDQuery.java

if [ $? -ne 0 ]; then
        exit 1
fi

echo "java -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/lib/sqlitejdbc-v056.jar:${DAX_GEN_DIR}/lib/opensha-commons-1.1.4.jar:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar org/scec/cme/cybershake/dax3/CyberShake_Stochastic_DAXGen ${RUN_ID_STRING} `pwd`/${SITE}_Stoch_dax/run_${RUN_ID} ${LF_ID} ${OPT_ARGS}"
java -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/lib/sqlitejdbc-v056.jar:${DAX_GEN_DIR}/lib/opensha-commons-1.1.4.jar:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar org/scec/cme/cybershake/dax3/CyberShake_Stochastic_DAXGen ${RUN_ID_STRING} `pwd`/${SITE}_Stoch_dax/run_${RUN_ID} ${LF_ID} ${OPT_ARGS}

if [ $? -ne 0 ]; then
        exit 1
fi

mv CyberShake_Stoch_${SITE}*.dax ${SITE}_Stoch_dax/run_${RUN_ID}
if [ -e ${SITE}.db ]; then
	mv ${SITE}.db ${SITE}_Stoch_dax/run_${RUN_ID}
fi

