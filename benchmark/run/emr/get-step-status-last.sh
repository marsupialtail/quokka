#!/bin/bash

# Assumes most recently launched cluster
. $WORKING_PATH/get-cluster_id.sh "WAITING STARTING TERMINATED"

STEP_JSON=$(aws emr list-steps --cluster-id $CLUSTER_ID)

STEP_INFO=$(echo $STEP_JSON | \
    python3 -c "import sys, json; recent_step=json.load(sys.stdin)['Steps'][0]; \
    print('Step Id: ', recent_step['Id'], ', Name: ', recent_step['Name'], \
    ', Status: ', recent_step['Status']['State'])")

echo $STEP_INFO