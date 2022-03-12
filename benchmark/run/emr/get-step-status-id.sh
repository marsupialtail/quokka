#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Illegal number of parameters"
    exit 1
fi

STEP_ID=$1

# Assumes most recently launched cluster
# TO-DO:  allow to input cluster_id as well
. ${WORKING_PATH}/get-cluster_id.sh "WAITING STARTING TERMINATED"

STEP_JSON=$(aws emr describe-step --cluster-id ${CLUSTER_ID} --step-id ${STEP_ID})

STEP_INFO=$(echo ${STEP_JSON} | \
    python3 -c "import sys, json; recent_step=json.load(sys.stdin)['Step']; \
    print('Step Id: ', recent_step['Id'], ', Name: ', recent_step['Name'], \
    ', Status: ', recent_step['Status']['State'])")

echo $STEP_INFO