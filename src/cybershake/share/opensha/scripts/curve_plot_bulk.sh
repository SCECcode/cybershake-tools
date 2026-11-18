#!/bin/bash

#This script takes in a file in the format:
#<site> <run id> <run id to compare 1> <run id to compare 2> ...
#and calculates curves for all.

if [ $# -lt 1 ]; then
	echo "Usage: $0 <input file>"
	exit 1
fi

input_file=$1

while read -r line; do
	read -ra linearr <<< $line
	if [ ${#linearr[@]} -lt 2 ]; then
		echo "Each line must have at least <site> <run_id>.  Aborting."
		exit 2
	fi
	site=${linearr[0]}
	run_id=${linearr[1]}
	comp_to=""
	if [ ${#linearr[@]} -ge 3 ]; then
		comp_to=${linearr[2]}
		for i in `seq 3 $((${#linearr[@]}-1))`; do
			comp_to="$comp_to,${linearr[$i]}"
		done
	fi
	cmd="/home/shock-ssd/scottcal/opensha/curve_plot_wrapper.sh --site $site --run-id $run_id --erf-file /home/shock-ssd/scottcal/opensha/conf/erf62_params.xml  --atten-rel-file /home/shock-ssd/scottcal/opensha/conf/ask2014.xml,/home/shock-ssd/scottcal/opensha/conf/bssa2014.xml,/home/shock-ssd/scottcal/opensha/conf/cb2014.xml,/home/shock-ssd/scottcal/opensha/conf/cy2014.xml   --period 3,5,10   --output-dir /home/shock/scottcal/db_products/curves/${site}   --type pdf,png   --force-add   --cmp RotD50   --password-file /home/shock/scottcal/runs/config/db_pass.txt -novm"
	if [ $comp_to!="" ]; then
		cmd="$cmd --compare-to ${comp_to}"
	fi
	$cmd
done < $input_file
