#!/bin/bash

#First argument is the velocity file
VEL_FILE=$1
shift 1
VS30=`grep Vs30 $VEL_FILE | cut -d "=" -f2`

cmd="/home/shock-ssd/scottcal/opensha/curve_plot_wrapper.sh --force-vs30 $VS30 $@"
echo "$cmd"
$cmd
