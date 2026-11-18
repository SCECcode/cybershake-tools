#!/bin/bash

set -o errexit

cd /home/scec-00/cybershk/opensha/git

echo "Updating from GitHub"
./pull_all.sh

echo "Compiling"
./build.sh
