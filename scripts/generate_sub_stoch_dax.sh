#!/bin/bash

DAX_GEN_DIR=/home/scec-02/cybershk/runs/dax-generator
JAVA_HOME=/usr/local/java/default

$JAVA_HOME/bin/javac -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar:${DAX_GEN_DIR}/lib/opensha-commons-all.jar  ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/DBConnect.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/RunIDQuery.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/CyberShake_Sub_Stoch_DAXGen.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/Stochastic_DAXParameters.java

$JAVA_HOME/bin/java -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar:${DAX_GEN_DIR}/lib/opensha-commons-all.jar org/scec/cme/cybershake/dax3/CyberShake_Sub_Stoch_DAXGen $@

