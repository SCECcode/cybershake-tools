#!/bin/bash

SITE=$1

javac -classpath .:dax-generator:dax-generator/lib/mysql-connector-java-5.0.5-bin.jar:dax-generator/lib/pegasus.jar:dax-generator/lib/globus_rls_client.jar:dax-generator/lib/commons-cli-1.1.jar dax-generator/org/scec/cme/cybershake/dax/CyberShakeRobMultipleDAXes.java dax-generator/org/scec/cme/cybershake/dax/CyberShakeDBProductsDAXGen.java

if [ $? -ne 0 ]; then
	exit 1
fi

java -classpath .:dax-generator:dax-generator/lib/mysql-connector-java-5.0.5-bin.jar:dax-generator/lib/pegasus.jar:dax-generator/lib/globus_rls_client.jar:dax-generator/lib/commons-cli-1.1.jar org/scec/cme/cybershake/dax/CyberShakeRobMultipleDAXes $@

if [ $? -ne 0 ]; then
	exit 1
fi

mkdir ${SITE}_PP_dax
mv CyberShake_${SITE}_*.dax ${SITE}_PP_dax/
mv CyberShake_${SITE}.pdax ${SITE}_PP_dax/
