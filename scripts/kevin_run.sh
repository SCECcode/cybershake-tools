#!/bin/bash

#./kevin_create_pp_wf.sh -s PAS -v vsi -e 51 -r 8 -g 6 -f 0.5 -q 1.0 --server moment.usc.edu -ps bluewaters -ds -nb -r -dh
#./kevin_create_pp_wf.sh -s SBSM -v vsi -e 51 -r 8 -g 6 -f 0.5 -q 1.0 --server moment.usc.edu -ps bluewaters -ds -nb -r -dh
#./kevin_create_pp_wf.sh -s WNGC -v vsi -e 51 -r 8 -g 6 -f 0.5 -q 1.0 --server moment.usc.edu -ps bluewaters -ds -nb -r -dh
#./kevin_create_pp_wf.sh -s STNI -v vsi -e 51 -r 8 -g 6 -f 0.5 -q 1.0 --server moment.usc.edu -ps bluewaters -ds -nb -r -dh
#./kevin_create_pp_wf.sh -s SMCA -v vsi -e 51 -r 8 -g 6 -f 0.5 -q 1.0 --server moment.usc.edu -ps bluewaters -ds -nb -r -dh
#./kevin_plan_pp.sh PAS 7015 bluewaters kmilner kmilner@usc.edu
#./kevin_plan_pp.sh SBSM 7016 bluewaters kmilner kmilner@usc.edu
#./kevin_plan_pp.sh WNGC 7017 bluewaters kmilner kmilner@usc.edu
#./kevin_plan_pp.sh STNI 7018 bluewaters kmilner kmilner@usc.edu
#./kevin_plan_pp.sh SMCA 7019 bluewaters kmilner kmilner@usc.edu
#./kevin_run_pp.sh -n kmilner@usc.edu -r USC 7014
#./kevin_run_pp.sh -n kmilner@usc.edu -r PAS 7015
./kevin_run_pp.sh -n kmilner@usc.edu -r SBSM 7016
./kevin_run_pp.sh -n kmilner@usc.edu -r WNGC 7017
./kevin_run_pp.sh -n kmilner@usc.edu -r STNI 7018
#./kevin_run_pp.sh -n kmilner@usc.edu -r SMCA 7019
