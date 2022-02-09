#!/bin/bash

if [ "$#" -ne 1 ]; then
    CLUSTER_STATUS='WAITING STARTING RUNNING'
else
    CLUSTER_STATUS=$1
fi

# Can expand to select right emr cluster by name parameter passed to `launch-emr.sh`.  
# Right now just takes the most recent cluser.

CLUSTER_INFO=$(aws emr list-clusters --cluster-states $CLUSTER_STATUS)

CLUSTER_ID=$(echo $CLUSTER_INFO | \
    python3 -c "
import sys, json
try:
    print(json.load(sys.stdin)['Clusters'][0]['Id'])
except IndexError:
    print('No cluster meets status requirements')"
)

echo 'Cluster Id: '$CLUSTER_ID