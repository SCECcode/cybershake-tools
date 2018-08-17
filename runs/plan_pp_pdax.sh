#!/bin/bash

if [ $# -ne 2 ]; then
	echo "Usage: $0 <site> <remote_site>"
	echo "Example: $0 USC abe"
	exit 1
fi

SITE=$1
REMOTE_SITE=$2
PDAX=CyberShake_${SITE}.pdax

cd ${SITE}_PP_dax

# Create empty notify file and add to LFN
NOTIFY_FILE="${SITE}_PP_notify.list"
PFN="gsiftp://shock.usc.edu/home/scec-00/cybershk/runs/notify/${NOTIFY_FILE}"
echo "Creating empty notify file ${NOTIFY_FILE}"
echo -e "" > ../notify/${NOTIFY_FILE}

# Create LFN entry for remote execution host
echo "Registering ${NOTIFY_FILE} in RLS"
line=`globus-rls-cli query lrc lfn ${NOTIFY_FILE} rls://shock.usc.edu`
if [ $? != 0 ]; then
    echo "LFN not found, registering new entry"
    globus-rls-cli create ${NOTIFY_FILE} ${PFN} rls://shock.usc.edu
else
    echo "LFN found, updating existing entry"
    oldpfn=${line#*:}
    globus-rls-cli rename pfn ${oldpfn} ${PFN} rls://shock.usc.edu
fi
# Shock isn't visible from hpc-scec, so add pool for hpc
globus-rls-cli attribute add $PFN pool pfn string hpc rls://shock.usc.edu

# Apparently the RLS update is not immediate
echo "Waiting for RLS to update"
sleep 5

# Modify the planning properties file
propfile=/home/scec-00/cybershk/config/properties
cp $propfile properties.dax
echo "pegasus.dir.storage.deep = true" >> properties.dax
newpropfile=`pwd`/properties.dax

pegasus-plan -Dpegasus.properties=${newpropfile} --rescue 100 -P ${PDAX} -s $REMOTE_SITE -o shock -D dags -f --cluster horizontal -vvvvv --nocleanup | tee log-plan-${PDAX}-${REMOTE_SITE}

# Modify the run-time user properties file
line=`grep pegasus-run log-plan-${PDAX}-${REMOTE_SITE} | head -n 1 | cut -d " " -f2`
propfile=${line#*=}
cp $propfile ${propfile}.pdax
echo "pegasus.dagman.maxjobs=12" >> ${propfile}.pdax
echo "pegasus.dir.storage.deep = true" >> ${propfile}.pdax

sed -i "s|${propfile}|${propfile}.pdax|g" log-plan-${PDAX}-${REMOTE_SITE}
