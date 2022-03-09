# Running Benchmarks

## EMR

You can either run the complete launch and query running process via `run-emr-complete.sh`.  Right now this will kick off a rather large series of processes that runs all the current EMR PySpark benchmarking.

If you do not run everything via the one `run-emr-complete.sh`, then you will first need to update and run the following three lines in the shell (or add them to your profile):
```
export REPO_PATH="!!INSERT THE PATH TO THE QUOKKA REPO HERE!!"
export WORKING_PATH="${REPO_PATH}/benchmark/run/emr"
export QUERY_PATH="${REPO_PATH}/benchmark/query"
```

Then you can run `launch-emr.sh` to launch the EMR cluster (this is always where parameters can be modified to adjust the size / type of the cluster).  Then you can run the desired step file, as should be stored in the `quokka/benchmark/run/emr/step` folder.  Right now all are set to "production" settings so each runs 30 trials.  It is recommended to decrease this before experimenting.

To add a new query, you will need to make both a python file in `quokka/benchmark/query/pyspark` and a EMR step file in `quokka/benchmark/run/emr/step`.  An example step file such as `quokka/benchmark/run/emr/step/emr-pyspark-tpch_06.sh` contains comments at the top about what to modify in the following for other queries.  PLEASE FOLLOW THE NAMING CONVENTION CLOSELY!!

Similarly, for the python file, the query itself will need to be modified.  Then if the query uses different tables, the input arguments, inputs to the executing function, the provided schema, and the dataframe creation will need to be modified.  This should still be pretty straightforward and systematic at this point.


## Analysis