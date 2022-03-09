#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Illegal number of parameters"
    exit 1
fi

export REPO_PATH="$1"
export WORKING_PATH="${REPO_PATH}/benchmark/run/emr"
export QUERY_PATH="${REPO_PATH}/benchmark/query"

bash $WORKING_PATH/launch-emr.sh $REPO_PATH

. $WORKING_PATH/get-cluster_id.sh "WAITING STARTING RUNNING"

bash $WORKING_PATH/step/emr-pyspark-join_00.sh

bash $WORKING_PATH/step/emr-pyspark-tpch_06.sh

bash $WORKING_PATH/step/emr-pyspark-tpch_12.sh