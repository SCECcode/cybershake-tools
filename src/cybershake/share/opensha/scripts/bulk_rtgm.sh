#!/bin/bash

set -x
IFS=' '
while read line; do
	site=`echo $line | cut -d" " -f1`
	run_id=`echo $line | cut -d" " -f2`
	#Calc hazard curve
	/home/scec-00/cybershk/opensha/curve_plot_wrapper.sh --site $site --run-id $run_id --erf-file /home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/MeanUCERF.xml --atten-rel-file /home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/cb2008.xml,/home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/ba2008.xml,/home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/cy2008.xml,/home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/as2008.xml --period 2,3,4,5,7.5,10 --component RotD100 --output-dir /home/scec-00/cybershk/opensha/curves/$run_id --type pdf,png --force-add --password-file /home/scec-00/cybershk/config/db_pass.txt
	#Do RTGM
	./rtgm_calc_wrapper.sh --output-dir /home/scec-00/cybershk/opensha/rtgm -af /home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/cb2008.xml,/home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/ba2008.xml,/home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/cy2008.xml,/home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/as2008.xml,/home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/idriss2014.xml,/home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/cb2014.xml,/home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/bssa2014.xml,/home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/cy2014.xml,/home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/ask2014.xml -ef /home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/MeanUCERF.xml -R ${run_id} --force-add RotD100:1,1.5 --type png,pdf --velocities
done < $1
