<p style="text-align:center;"><img src="quokka2.png" alt="Logo"></p>

# **If you like, please**: <iframe src="https://ghbtns.com/github-btn.html?user=marsupialtail&repo=quokka&type=star&count=true&size=large" frameborder="0" scrolling="0" width="170" height="30" title="GitHub"></iframe>

## **Introduction** 

Quokka is a lightweight distributed dataflow engine written completely in Python targeting modern data science use cases involving 100GBs to TBs of data. At its core, Quokka manipulates streams of data with stateful actors. **Quokka offers a stream-centric, Python-native perspective to Spark.** Instead of logical **DataFrames** where partitions are present across the cluster at the same time, Quokka operates on **DataStreams**, where partitions may be materialized in sequence. Please see the [Getting Started](started.md) for more details.

Quokka's DataStream interface is highly inspired by the [Polars](https://pola-rs.github.io/polars/py-polars/html/reference/index.html) API. One simple way to think about Quokka is **distributed Polars on Ray**. Quokka currently supports a large percentage of Polars APIs, though keeping up with Polars' rapid development is a challenge. Quokka also offers SQL analogs to some of the Polars APIs, e.g. `filter_sql` or `with_columns_sql` that are helpful to people with more of a SQL background.

This streaming paradigm inspired by high performance databases such as DuckDB and Snowflake allows Quokka to greatly outperform Apache Spark performance on SQL type workloads reading from cloud storage for formats like CSV and Parquet.

<p align="center">
  <img src="tpch-parquet.svg" />
</p>

<sub>Fineprint: benchmark done with TPC-H Scale Factor 100 in Parquet format using four r6id.2xlarge instances for Quokka and EMR 6.8.0 with five r6id.2xlarge instances for Spark where one instance is used as a coordinator. Ignores the time to start up the SparkSQL or Quokka runtimes, which is comparable. Did not compare against Modin or Dask since they are typically much slower than SparkSQL.</sub>

What's even better than being cheap and fast is the fact that since Quokka is Python native, you can easily use your favorite machine learning libraries like Scikit-Learn and Pytorch with Quokka inside of arbitrary Python functions to transform your DataStreams.

Another great advantage is that a streaming data paradigm is more in line with how data arrives in the real world, making it easy to bridge your data application to production, or conduct time-series backfilling on your historical data.

You develop with Quokka locally, and deploy to cloud (currently AWS) with a single line of code change. You can spin up a new cluster or connect to an existing Ray cluster. Kubernetes can be supported through setting up a Ray cluster with [KubeRay](https://github.com/ray-project/kuberay). Like Spark, Quokka can **recover from worker failures**, but expects a reliable coordinator node that doesn't fail throughout the duration of the job. Like Spark, Quokka benefits from having plenty of RAM, and fast SSDs for spilling if needed. 

## **Roadmap**

1. **Streaming support.** Although Quokka follows a streaming model, it currently does not support "streaming" computations from Kafka, Kinesis etc. They will soon be supported. This will allow batch data pipelines to be deployed to production with one line code change. Target Q2 2023.
2. **Better SQL support.** Currently Quokka has very rudimentary SQL support in the form of qc.sql(SQL_QUERY). It will loudly complain if it sees some SQL that it doesn't understand, e.g. complicated nested subqueries etc. I am working with [SQLGlot](https://github.com/tobymao/sqlglot) to enhancing SQL support. Target pass TPC-H and say 75% of TPC-DS Q2 2023.

## **Bug Bounty**

If you see any bugs, please raise a Github issue. I will reward you with a $10 Amazon gift card if you find a bug that I can't fix within a week. If you fix the bug yourself and make a PR, you will get another reward: 

- $10 for a bug that results in a runtime error.
- $50 for a bug that results in a silent error, e.g. produces wrong results.

## **Contact**
If you are interested in trying out Quokka or hit any problems at all, please contact me at zihengw@stanford.edu or [Discord](https://discord.gg/6ujVV9HAg3). I will try my best to make Quokka work for you.