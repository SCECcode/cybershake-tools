#!/bin/bash

DAX_GEN_DIR=/home/shock/scottcal/runs/cybershake-tools/dax-generator-3
JAVA_HOME=/home/shock-ssd/scottcal/java/default

$JAVA_HOME/bin/javac -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar:${DAX_GEN_DIR}/lib/opensha-cybershake-all.jar:/home/shock-ssd/scottcal/opensha/opensha-all.jar ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/DBConnect.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/RunIDQuery.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/CyberShake_Sub_Stoch_DAXGen.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/Stochastic_DAXParameters.java

#$JAVA_HOME/bin/javac -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar:${DAX_GEN_DIR}/lib/opensha-commons-all.jar  ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/DBConnect.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/RunIDQuery.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/CyberShake_Sub_Stoch_DAXGen.java ${DAX_GEN_DIR}/org/scec/cme/cybershake/dax3/Stochastic_DAXParameters.java

#$JAVA_HOME/bin/java -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar:${DAX_GEN_DIR}/lib/opensha-commons-all.jar org/scec/cme/cybershake/dax3/CyberShake_Sub_Stoch_DAXGen $@

$JAVA_HOME/bin/java -classpath .:${DAX_GEN_DIR}:${DAX_GEN_DIR}/lib/snakeyaml-1.25.jar:${DAX_GEN_DIR}/lib/jackson-coreutils-1.8.jar:${DAX_GEN_DIR}/lib/jackson-annotations-2.9.10.jar:${DAX_GEN_DIR}/lib/jackson-databind-2.9.10.jar:${DAX_GEN_DIR}/lib/jackson-dataformat-yaml-2.9.10.jar:${DAX_GEN_DIR}/lib/jackson-core-2.9.10.jar:${DAX_GEN_DIR}/lib/mysql-connector-java-5.0.5-bin.jar:${DAX_GEN_DIR}/lib/pegasus.jar:${DAX_GEN_DIR}/lib/globus_rls_client.jar:${DAX_GEN_DIR}/lib/commons-cli-1.1.jar:/home/shock-ssd/scottcal/opensha/opensha-all.jar org/scec/cme/cybershake/dax3/CyberShake_Sub_Stoch_DAXGen $@
