#!/bin/bash

if [ $# -ne 3 ]; then
	echo "Usage: $0 <SGT directory prefix> <site> <run_ID>"
	exit 1
fi

DIR=$1
SITE=$2
RUN_ID=$3

globus-rls-cli create ${SITE}_fx_${RUN_ID}.sgt ${DIR}/${SITE}_fx.sgt rls://shock.usc.edu
globus-rls-cli create ${SITE}_fx_${RUN_ID}.sgt.md5 ${DIR}/${SITE}_fx.sgt.md5 rls://shock.usc.edu
globus-rls-cli create ${SITE}_fy_${RUN_ID}.sgt ${DIR}/${SITE}_fy.sgt rls://shock.usc.edu
globus-rls-cli create ${SITE}_fy_${RUN_ID}.sgt.md5 ${DIR}/${SITE}_fy.sgt.md5 rls://shock.usc.edu

globus-rls-cli attribute add ${DIR}/${SITE}_fx.sgt pool pfn string hpc rls://shock.usc.edu
globus-rls-cli attribute add ${DIR}/${SITE}_fx.sgt.md5 pool pfn string hpc rls://shock.usc.edu
globus-rls-cli attribute add ${DIR}/${SITE}_fy.sgt pool pfn string hpc rls://shock.usc.edu
globus-rls-cli attribute add ${DIR}/${SITE}_fy.sgt.md5 pool pfn string hpc rls://shock.usc.edu
