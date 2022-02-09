#!/bin/bash

# If you run this file directly from the command line then run the following in the shell first
# export REPO_PATH="!!INSERT THE PATH TO THE QUOKKA REPO HERE!!"
# export WORKING_PATH="${REPO_PATH}/benchmark/run/emr"
# export QUERY_PATH="${REPO_PATH}/benchmark/query"

# Modify these two variables and line 21 which indicates which tables to read from
# If more than one table, add another line with a different parameter name below and in the query's python file
EXP_NAME=emr-pyspark-join_00
TRIALS=30

aws s3 cp "${QUERY_PATH}/pyspark/${EXP_NAME}.py" "s3://quokka-benchmark/${EXP_NAME}.py"
. ${WORKING_PATH}/get-cluster_id.sh

if [ "${CLUSTER_ID}" == "No cluster meets status requirements" ]; 
then
    echo "Please launch a cluster."
else
    for (( i=0; i<$TRIALS; i++ ))
    do
        aws emr add-steps \
        --cluster-id ${CLUSTER_ID} \
        --steps Type=Spark,Name="${EXP_NAME}_$i",ActionOnFailure=CONTINUE,\
Args=[s3://quokka-benchmark/${EXP_NAME}.py,\
--data_source_a,s3://yugan/a-big.csv,\
--data_source_b,s3://yugan/b-big.csv,\
--output_uri,"s3://quokka-benchmark/experiment/${EXP_NAME}/$i"]
    done
fi