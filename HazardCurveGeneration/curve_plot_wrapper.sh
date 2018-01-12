#!/bin/bash

cd /home/scec-00/cybershk/opensha/OpenSHA

set -o errexit

java="/usr/java/jdk1.6.0_10/bin/java"

if [ ! -x $java ];then
	java="java"
fi

#classpath=".:./classes:lib/mysql-connector-java-3.1.6-bin.jar:lib/commons-cli-1.2.jar:lib/dom4j-1.6.1.jar:lib/jfreechart-1.0.13.jar:lib/jcommon-1.0.5.jar:lib/itext-1.3.jar:lib/poi-2.5.1-final-20040804.jar:lib/nnls.jar:lib/f2jutil.jar:lib/commons-lang-2.4.jar"
classpath=".:./classes:lib/commons-cli-1.2.jar"
$java -cp $classpath org.opensha.sha.cybershake.plot.HazardCurvePlotter $@
