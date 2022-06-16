<p align="center">
  <img src="https://github.com/marsupialtail/quokka/blob/master/docs/quokka-banner.png?raw=true" alt="Title"/>
</p>

Docs: https://marsupialtail.github.io/quokka/started/

Quokka is a fast data processing engine whose core consists of ~1000 lines of Python code. However it can be leveraged to obtain near-peak performance on SQL queries on data "lakes" with CSV and Parquet file formats. It is often several times faster than SparkSQL and an order of magnitude faster than Dask.

Quokka stands on the shoulders of giants. It uses [Ray](https://github.com/ray-project/ray) for task scheduling, Redis for lineage logging, [Polars](https://github.com/pola-rs/polars) for relational algebra kernels and [Apache Arrow](https://github.com/apache/arrow) for I/O. All of those are backed by efficient C++/Rust implementations. Quokka is a high-performance way of piecing them all together.

The core Quokka runtime API allows you to construct a computation DAG with stateful executors. *Any* data source can be used, CSV/Parquet on Disk/S3 (apps/tpch-), Iceberg/DeltaLake/Hudi, bucket of images (apps/pinecone.py), Ethereum blockchain through web3 APIs, transactional database CDC endpoints etc. If you have some esoteric data source that you want to run analytics on, please send me a challenge as a Github issue. 

Please refer to the docs and examples in the apps directory. In the future we will support a dataframe API and hopefully SQL. The plan is to use this amazing repo: [sqlglot](https://github.com/tobymao/sqlglot).

For any questions/concerns/just want to chat: zihengw@stanford.edu

# Quickstart

**Local**: 
1) Pull the repo, cd into it and do:
```pip3 install .```
2) now try running apps/tutorials/lesson0.py

**Distributed**:
1) Pick a node to be the head node.
2) You only have to pull the repo and install on the head node.
3) You need to start a redis server instance on each machine. make sure to use the redis.conf in the repo. This is kinda annoying. We are / I am going to phase out Redis within the next few months and this should not be a problem.
4) start a ray head node locally: ```ray start --head --port=6379```. On each worker node do the command ray start tells you to do in next steps.

Alternatively, you can use the pyquokka.utils.

Now check out some of the applications in the apps directory.

Please raise a Github issue if you encounter any issues!
