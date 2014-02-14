#!/bin/bash

if [ $# -ne 2 -a $# -ne 4 ]; then
        echo "Usage: $0 <site> <erf> [sgt_var] [rup_var]"
        echo "Example: $0 USC 35 5 3"
        exit 1
fi

SITE=$1
ERF=$2
if [ $# -eq 4 ]; then
    SGT_VAR=$3
    RUP_VAR=$4
else
    SGT_VAR="5"
    RUP_VAR="3"
fi


# Compile DAX generator
javac -classpath .:dax-generator:dax-generator/lib/mysql-connector-java-5.0.5-bin.jar:dax-generator/lib/pegasus.jar:dax-generator/lib/globus_rls_client.jar:dax-generator/lib/commons-cli-1.1.jar dax-generator/org/scec/cme/cybershake/dax/CyberShakeSGTWorkflow.java

if [ $? -ne 0 ]; then
	exit 1
fi

# Run DAX generator
java -classpath .:dax-generator:dax-generator/lib/mysql-connector-java-5.0.5-bin.jar:dax-generator/lib/pegasus.jar:dax-generator/lib/globus_rls_client.jar:dax-generator/lib/commons-cli-1.1.jar org/scec/cme/cybershake/dax/CyberShakeSGTWorkflow ${SITE} ${ERF}

if [ $? -ne 0 ]; then
	exit 1
fi

mkdir ${SITE}_SGT_dax
mv CyberShake_SGT_${SITE}.dax ${SITE}_SGT_dax/
