<p style="text-align:center;"><img src="quokka2.png" alt="Logo"></p>

## If you like, please: <iframe src="https://ghbtns.com/github-btn.html?user=marsupialtail&repo=quokka&type=star&count=true&size=large" frameborder="0" scrolling="0" width="170" height="30" title="GitHub"></iframe>

## Introduction 

Quokka is a lightweight distributed dataflow engine written completely in Python targeting modern data science use cases involving 100GBs to TBs of data. At its core, Quokka manipulates streams of data with stateful actors. **Quokka offers a stream-centric, Python-native perspective to tasks commonly done today by Spark.** Please see the [Getting Started](started.md) for further details.

This streaming paradigm inspired by high performance databases such as DuckDB and Snowflake allows Quokka to greatly outperform Apache Spark performance on SQL type workloads reading from cloud blob storage like S3 for formats like CSV and Parquet.

![Quokka Stream](tpch-parquet.svg)

<sub>Fineprint: benchmark done using four c5.4xlarge instances for Quokka and EMR 6.5.0 with five c5.4xlarge instances for Spark where one instance is used as a coordinator. Ignores initialization costs which are generally comparable between Quokka and Spark.</sub>

What's even better than being cheap and fast is the fact that since Quokka is Python native, you can easily use your favorite machine learning libraries like Scikit-Learn and Pytorch with Quokka inside of arbitrary Python functions to transform your DataStreams.

Another great advantage is that a streaming data paradigm is more in line with how data arrives in the real world, making it easy to bridge your data application to production, or conduct time-series backfilling on your historical data.

You develop with Quokka locally, and deploy to cloud (currently AWS) with a single line of code change. Quokka is specifically designed for the following workloads.

1. **SQLish data engineering workloads on data lake.** You can try Quokka if you want to speed up some Spark data jobs, or if you want to implement "stateful Python UDFs" in your SQL pipeline, which is kind of a nightmare in Spark. (e.g. forward computing some feature based on historical data) Quokka can also typically achieve much better performance than Spark on pure SQL workloads when input data comes from cloud storage, especially if the data is in CSV format.

    **The drawback is Quokka currently does not support SQL interface, so you are stuck with a dataframe-like DataStream API.** However SQL optimizations such as predicate pushdown and early projection are implemented.

2. (support forthcoming) **ML engineering pipelines on large unstructured data datasets.** Since Quokka is Python-native, it interfaces perfectly with the Python machine learning ecosystem. **No more JVM troubles.** Unlike Spark, Quokka also will let you precisely control the placement of your stateful operators on machines, preventing GPU out-of-memory and improving performance by reducing contention. Support for these workloads are still in the works. If you are interested, please drop me a note: zihengw@stanford.edu or [Discord](https://discord.gg/YKbK2TVk).

## Roadmap

1. **Streaming support.** Although Quokka follows a streaming model, it currently does not support "streaming" computations from Kafka, Kinesis etc. They will soon be supported. This will allow batch data pipelines to be deployed to production with one line code change. Target Q4 2022.
2. **Fault tolerance.** Currently Quokka's fault tolerance mechanism is experimental. Improvements are being made in this direction transparent to the API. Please use on-demand instances for important workloads. (Well if you are planning on using Quokka for important workloads or any workload, please contact me: zihengw@stanford.edu.) The goal is to support Spark-like fault recovery stability by Q1 2023.
3. **Full SQL support.** I want to be able to do qc.sql(SQL_QUERY). I am working with [SQLGlot](https://github.com/tobymao/sqlglot) to make this happen. Target pass TPC-H and say 75% of TPC-DS Q1 2023.
4. **Time Series Package.** Quokka will support point-in-time joins and asof joins natively by Q4 2022. This will be useful for feature backtesting, etc.

## Contact
If you are interested in trying out Quokka or hit any problems (any problems at all), please contact me at zihengw@stanford.edu or [Discord](https://discord.gg/YKbK2TVk). I will try my best to make Quokka work for you.