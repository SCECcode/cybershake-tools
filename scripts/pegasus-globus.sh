#!/bin/bash

#USC and OLCF
export PYTHONPATH=/home1/scottcal/.local/lib/python3.9/site-packages/:$PYTHONPATH
python3 /home/shock-ssd/scottcal/pegasus/default/lib64/pegasus/python/Pegasus/cli/pegasus-globus-online-init.py -p -c 56569ec1-af41-4745-a8d1-8514231c7a6d -d sso.ccs.ornl.gov
#pegasus-globus-online-init -p -c 56569ec1-af41-4745-a8d1-8514231c7a6d -d sso.ccs.ornl.gov
#Frontera
python3 /home/shock-ssd/scottcal/pegasus/default/lib64/pegasus/python/Pegasus/cli/pegasus-globus-online-init.py -p -c bec0eec6-d29d-4447-9813-cd9751c199e9
