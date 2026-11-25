#!/bin/bash

cd /home/scec-00/cybershk/opensha

set -o errexit

if [[ $# -lt 1 ]];then
	echo "must supply class to run (and optional arguments)"
	exit 2
fi

java="/usr/local/java/default/jre/bin/java"

if [ ! -x $java ];then
	java="java"
fi

#classpath="git/opensha-cybershake/build/libs/opensha-cybershake-all.jar"
classpath="opensha-cybershake-all-z.jar"
$java -cp $classpath $@
