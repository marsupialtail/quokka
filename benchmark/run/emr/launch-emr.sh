#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Illegal number of parameters"
    exit 1
fi

export REPO_PATH=$1
export WORKING_PATH=${REPO_PATH}/benchmark/run/emr
export QUERY_PATH=${REPO_PATH}/benchmark/query

aws emr create-cluster \
    --name "spinup" \
    --release-label emr-6.5.0 \
    --applications Name=Spark Name=Hadoop \
    --ec2-attributes KeyName="oregon-neurodb",AvailabilityZone="us-west-2a" \
    --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=c5.2xlarge \
    --log-uri "s3://quokka-benchmark/logs/" \
    --use-default-roles \
    --no-termination-protected \
    --auto-termination-policy IdleTimeout=1800