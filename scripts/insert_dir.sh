#!/bin/bash

set -o errexit

if [ $# -lt 1 ]; then
	echo "Usage: ./insert_dir.sh args"
	exit 1
fi

#SITE=$1
#SGT_VAR=$2
#SERVER=$3
#RUP_VAR_SCEN_ID=$4
#ERF_ID=$5
#AMPS_PATH=$6


export CYBERCOMMANDS_JARS='/home/shock-ssd/scottcal/CyberCommands/lib'

#./CyberLoadAmps_SC -site $SITE -sgt $SGT_VAR -server $SERVER -rvid $RUP_VAR_SCEN_ID -erfid $ERF_ID -p $AMPS_PATH
/home/shock-ssd/scottcal/CyberCommands/bin/CyberLoadAmps_SC "$@"
