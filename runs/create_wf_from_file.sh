#!/bin/bash

if [ $# -lt 4 ]; then
	echo "Usage: $0 <site> <erf_id> <rup_var_scenario_id> <input_file>"
	exit -1
fi

SITE=$1
ERF=$2
RV=$3
FILE=$4

javac -classpath .:dax-generator:dax-generator/lib/mysql-connector-java-5.0.5-bin.jar:dax-generator/lib/pegasus.jar:dax-generator/lib/globus_rls_client.jar:dax-generator/lib/commons-cli-1.1.jar dax-generator/org/scec/cme/cybershake/dax/CyberShakeRob.java

if [ $? -ne 0 ]; then
	exit 1
fi

java -classpath .:dax-generator:dax-generator/lib/mysql-connector-java-5.0.5-bin.jar:dax-generator/lib/pegasus.jar:dax-generator/lib/globus_rls_client.jar:dax-generator/lib/commons-cli-1.1.jar org/scec/cme/cybershake/dax/CyberShakeRob $SITE $ERF $RV -f $FILE

if [ $? -ne 0 ]; then
	exit 1
fi

mv CyberShake_${SITE}_file_${FILE}.dax ${SITE}_PP_dax/

