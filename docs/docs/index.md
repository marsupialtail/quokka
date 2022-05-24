<p style="text-align:center;"><img src="quokka2.png" alt="Logo"></p>

## If you like, please: <iframe src="https://ghbtns.com/github-btn.html?user=marsupialtail&repo=quokka&type=star&count=true&size=large" frameborder="0" scrolling="0" width="170" height="30" title="GitHub"></iframe>

## Introduction 

Quokka is a lightweight distributed dataflow engine written completely in Python targeting ML/data engineering use cases involving TBs of data. At its core, Quokka treats a data source as a stream of Python objects, and offers an API to operate on them with stateful executors. For example, a 10TB CSV file in S3 will be converted to a stream of PyArrow tables, while an S3 bucket with ten million images will be converted to a stream of bytearrays. You can then join the stream of PyArrow tables with another stream from another CSV file to do SQL, or run deep learning inference on the stream of images. You can define you own input data readers and stateful operators, or use Quokka's library implementations. **Quokka offers a stream-centric, Python-native perspective to tasks commonly done today by Spark.**

You develop with Quokka locally, and deploy to cloud (currently AWS) with a single line of code change. Quokka is specifically designed for the following workloads.

1. **UDF-heavy SQL data engineering workloads on data lake.** You can try Quokka if you are fed up with Spark's performance on some of your data pipelines, or if you want to implement "stateful Python UDFs" in your SQL pipeline, which is kind of a nightmare in Spark. (e.g. forward computing some feature based on historical data) **Quokka can also typically achieve much better performance than Spark on pure SQL workloads when input data comes from cloud storage, especially if the data is in CSV format.** However, Quokka currently does not offer a dataframe API or SQL interface, so for simple queries it can be harder to use than Spark. Please look at the Quokka implementations of TPC-H queries in the example directory to see how Quokka's current API works for SQL. 
2. **ML engineering pipelines on large unstructured data datasets.** Since Quokka is Python-native, it interfaces perfectly with the Python machine learning ecosystem. **No more JVM troubles.** Unlike Spark, Quokka also let's you precisely control the placement of your stateful operators on machines, preventing GPU out-of-memory and improving performance by reducing contention. 

## Roadmap

1. **Streaming support.** Although Quokka follows a streaming model, it currently does not support "streaming" computations from Kafka, Kinesis etc. They will soon be supported. 
2. **SQL/dataframe API**. Similar to how Spark exposes (or used to expose) a low-level RDD API and a high-level dataframe API, and how Tensorflow exposes lower-level graph construction APIs and high-level Keras/Estimator APIs, Quokka currently only exposes a low-level graph construction API. Mostly this means users have to implement their own physical plan for SQL workloads. While this can be tedious, it can offer much higher performance and better flexibility with UDFs etc. **We are actively working on a higher level dataframe API with a SQL interface.**
3. **Fault tolerance.** Currently Quokka's fault tolerance mechanism is experimental. Improvements are being made in this direction transparent to the API. Please use on-demand instances for important workloads. (Well if you are planning on using Quokka for important workloads or any workload, please contact me.)

## Contact
If you are interested in trying out Quokka or hit any problems (any problems at all), please contact me at zihengw@stanford.edu. I will try my best to make Quokka work with your use cases. Please help me make Quokka better!