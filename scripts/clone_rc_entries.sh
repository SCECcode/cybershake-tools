#!/bin/bash

#This script will create SGT entries for a new run ID, useful for running tests
#PFNs and pool info are retrieved using pegasus-rc-client

if [ $# -ne 3 ]; then
	echo "Usage: $0 <site> <old run_ID> <new run ID>"
	exit 1
fi

SITE=$1
OLD_RUN_ID=$2
NEW_RUN_ID=$3

#Determine old info, then create new
for comp in x y; do
	for suffix in sgt sgthead sgt.md5; do
		#pegasus-rc-client lookup SYL_fx_7672.sgt
		#SYL_fx_7672.sgt gsiftp://gridftp.ccs.ornl.gov/gpfs/alpine/scratch/callag/geo112/SGT_Storage/SYL/SYL_fx_7672.sgt site="summit"
		lfn=${SITE}_f${comp}_${OLD_RUN_ID}.${suffix}
		entry=`pegasus-rc-client lookup $lfn`
		if [[ $entry == "" ]]; then
			echo "No entry for $lfn"
			exit 1
		fi
		read -r -a array <<< $entry
		#Change LFN
		new_lfn=${SITE}_f${comp}_${NEW_RUN_ID}.${suffix}
		#Same PFN
		pfn=${array[1]}
		#Keep site attribute
		attr=${array[2]}
		echo "pegasus-rc-client insert $new_lfn $pfn $attr"
	done
done
