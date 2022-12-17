#!/bin/bash

#Create RC entries for the SGT files using the new IDs which point to the old IDs

if [ $# -lt 3 ]; then
	echo "Usage: $0 <site> <old run ID> <new run ID>"
	exit 1
fi

site=$1
old_ID=$2
new_ID=$3

for c in x y; do
	for suf in sgt sgthead sgt.md5; do
		#prc_cmd="pegasus-rc-client -Dpegasus.catalog.replica=JDBCRC -Dpegasus.catalog.replica.db.driver=sqlite -Dpegasus.catalog.replica.db.url=jdbc:sqlite:/home/shock-ssd/scottcal/workflow/RC.sqlite lookup ${site}_f${c}_${old_ID}.${suf}"
		prc_cmd="pegasus-rc-client lookup ${site}_f${c}_${old_ID}.${suf}"
		rc_entry=`$prc_cmd`
		pfn=`echo $rc_entry | cut -d" " -f2`
		pool=`echo $rc_entry | cut -d" " -f3`
		new_lfn=${site}_f${c}_${new_ID}.${suf}
		insert_cmd="pegasus-rc-client insert $new_lfn $pfn $pool"
		echo $insert_cmd
		$insert_cmd
	done
done
