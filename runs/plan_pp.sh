#!/bin/bash

if [ $# -ne 2 ]; then
	echo "Usage: $0 <dax> <remote_site>"
	echo "Example: $0 CyberShake_USC-0.dax abe"
	exit 1
fi

DAX=$1
REMOTE_SITE=$2

# Create empty notify file and add to LFN
NOTIFY_FILE="${REMOTE_SITE}_PP_notify.list"
echo "Creating empty notify file ${NOTIFY_FILE}"
echo -e "" > ../notify/${NOTIFY_FILE}

# Create LFN entry for remote execution host
echo "Registering ${NOTIFY_FILE} in RLS"
globus-rls-cli create ${NOTIFY_FILE} gsiftp://shock.usc.edu/home/scec-00/cybershk/runs/notify/${NOTIFY_FILE} rls://shock.usc.edu
#if [ $? -ne 0 ]; then
#        exit
#fi

# Apparently the RLS update is not immediate
echo "Waiting for RLS to update"
sleep 5

cmd="pegasus-plan -Dpegasus.properties=/home/scec-00/cybershk/config/properties -d $DAX -s $REMOTE_SITE -o hpc -D dags -f --cluster horizontal -v --nocleanup | tee log-plan-${DAX}-$REMOTE_SITE"
echo $cmd
