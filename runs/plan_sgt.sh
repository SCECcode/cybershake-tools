#!/bin/sh

if [ $# != 2 ]; then
	echo "Usage:  $0 cs_site remote_site"
	echo "Example: $0 USC abe_noglidein"
	exit 1
fi

CS_SITE=$1
REMOTE_SITE=$2

cd ${CS_SITE}_SGT_dax
DAX="CyberShake_SGT_${CS_SITE}.dax"

#if we use HPC, we need to convert RLS parameters
#host_count = host_xcount
#count = xcount * host_xcount
#if [ $REMOTE_SITE == "hpc" ]; then
#	sed -i 's/xcount/count/g' $DAX
#fi

# Create empty notify file and add to LFN
NOTIFY_FILE="${CS_SITE}_SGT_notify.list"
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

# Modify properties file
propfile=/home/scec-00/cybershk/config/properties
cp $propfile properties.dax
echo "pegasus.dir.storage=data/SgtFiles/${CS_SITE}" >> properties.dax
echo "pegasus.dir.storage.deep = false" >> properties.dax
newpropfile=`pwd`/properties.dax

echo "pegasus-plan -Dpegasus.properties=${newpropfile} -d $DAX -s $REMOTE_SITE -o ${REMOTE_SITE} -D dags -f -v --nocleanup | tee log-plan-${DAX}-${REMOTE_SITE}"
pegasus-plan -Dpegasus.properties=${newpropfile} -d $DAX -s $REMOTE_SITE -o $REMOTE_SITE -D dags -f -v --nocleanup | tee log-plan-${DAX}-${REMOTE_SITE}

