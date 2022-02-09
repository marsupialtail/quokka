#!/bin/bash

EXP_NAME=emr-tbl_to_parquet

aws s3 cp "$QUERY_PATH/pyspark/$EXP_NAME.py" "s3://quokka-benchmark/$EXP_NAME.py"
. $WORKING_PATH/get-cluster_id.sh

if [ "$CLUSTER_ID" == "No cluster meets status requirements" ]; 
then
    echo "Please launch a cluster."
else
    aws emr add-steps \
    --cluster-id $CLUSTER_ID \
    --steps Type=Spark,Name="$EXP_NAME_$i",ActionOnFailure=CONTINUE,Args=[s3://quokka-benchmark/$EXP_NAME.py]
fi