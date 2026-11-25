#!/bin/bash

function retval {
	rv=$1
	message=${2-"unknown component"}
	if [ $rv -ne 0 ];then
		echo "'${message}' returned non-zero exit code: $rv"
		echo "exiting with status $rv"
		exit $rv
	fi
}
