#!/bin/bash

set -x
IFS=' '
while read line; do
	site=`echo $line | cut -d" " -f1`
	run_id=`echo $line | cut -d" " -f2`
	./gmpe_deterministic_wrapper.sh -af /home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/cb2008.xml,/home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/ba2008.xml,/home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/cy2008.xml,/home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/as2008.xml,/home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/idriss2014.xml,/home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/cb2014.xml,/home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/bssa2014.xml,/home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/cy2014.xml,/home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/ask2014.xml -ef /home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf/MeanUCERF.xml --period 1,1.5,2,3,4,5,7.5,10 -R $run_id --output-dir /home/scec-00/cybershk/opensha/rtgm
done < $1
