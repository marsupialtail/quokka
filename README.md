<p align="center">
  <img src="https://github.com/marsupialtail/quokka/blob/master/docs/quokka-banner.png?raw=true" alt="Title"/>
</p>

Docs: https://marsupialtail.github.io/quokka/runtime/

Quokka is a fast data processing engine whose core consists of ~1000 lines of Python code. However it can be leveraged to obtain near-peak performance on SQL queries on data "lakes" with CSV and Parquet file formats. It is often several times faster than SparkSQL and an order of magnitude faster than Dask.

Quokka stands on the shoulders of giants. It uses Ray for task scheduling, Redis for pub/sub, Polar-rs for relational algebra kernels and Pyarrow for I/O. All of those are backed by efficient C++/Rust implementations. Quokka is merely a high-performance way of piecing them all together. In the long run, we will try to cut our dependency on Ray and Redis.

The core Quokka API allows you to construct a computation DAG with stateful executors. Please refer to the docs and examples in the apps directory. In the future we will support a dataframe API and hopefully SQL.

# Quickstart

**Local**: 
1) Pull the repo, cd into it and do pip3 install .
2) start a local redis server instance. you will need to download the redis-server binary to do this: 
```./redis-server ../redis.conf --port 6800 --protected-mode no& ``` with the redis.conf included in the Quokka repo.
4) start a ray cluster locally: ray start --head --port=6379


**Distributed**:
1) Pick a node to be the head node.
2) You only have to pull the repo and install on the head node.
3) You need to start a redis server instance on each machine. make sure to use the redis.conf in the repo. This is kinda annoying. We are / I am going to phase out Redis within the next few months and this should not be a problem.
4) start a ray head node locally: ray start --head --port=6379. On each worker node do the command ray start tells you to do in next steps.

Now check out some of the applications in the apps directory.
