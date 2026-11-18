#!/bin/bash

if [ $# -lt 2 ]; then
	echo "Usage: $0 <run_dir> <run_ID>"
	exit 1
fi

RUN_DIR=$1
RUN_ID=$2

JOBID=$(grep "submitted to cluster" "${RUN_DIR}"/log-run-CyberShake* | head -n 1 | awk '{print $6}' | sed "s/\.//")

SUBHOST=$(hostname -s)
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
if ! "${SCRIPT_DIR}/cybershake" run-manager edit "${RUN_ID}" "Job_ID=${SUBHOST}:${JOBID}"; then
	echo "Unable to update Job ID for run ${RUN_ID}"
	exit 2
fi

exit 0
