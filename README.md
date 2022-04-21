# Quokka Core
![alt text](https://github.com/marsupialtail/quokka/tree/master/docs/quokka.png?raw=true)

Docs: https://marsupialtail.github.io/quokka/runtime/

Quokka is a fast data processing engine whose core consists of ~1000 lines of Python code. However it can be leveraged to obtain near-peak performance on SQL queries on data "lakes" with CSV and Parquet file formats. It is often several times faster than SparkSQL and an order of magnitude faster than Dask.

Quokka stands on the shoulders of giants. It uses Ray for task scheduling, Redis for pub/sub, Polar-rs for relational algebra kernels and Pyarrow for I/O. All of those are backed by efficient C++/Rust implementations. Quokka is merely a high-performance way of piecing them all together.

The core Quokka API allows you to construct a computation DAG with stateful executors. Please refer to the docs and examples in the apps directory.
