<div align="center">
<p align="center">
  <img src="https://github.com/marsupialtail/quokka/blob/master/docs/quokka-banner.png?raw=true" alt="Title"/>

**Distributed, fault-tolerant, fast time-series processing engine for trillions of events.<br/>**
**Built on Python with DuckDB, Polars, Ray, Arrow, Redis and SQLGlot. Understands CSV, Parquet, Lance, Iceberg, Delta (WIP).**

<a href="https://discord.gg/6ujVV9HAg3" style="display:inline-block;">
    <img src="https://img.shields.io/badge/-Join%20Quokka%20Discord-blue?logo=discord" alt="Join Discord" height="25px"/>
</a>
<a href="https://marsupialtail.github.io/quokka/">
    <img src="https://github.com/marsupialtail/quokka/blob/master/docs/docs/badge.svg" alt="rust docs" height="25px"/>
</a>
<a href="https://pypi.org/project/pyquokka/">
    <img src="https://img.shields.io/pypi/v/pyquokka.svg" alt="PyPi Latest Release" height="25px"/>
</a>
</p>
</div>

If you would like to support Quokka, please give us a star! 🙏 

## Showcases

* **Tick-level backtesting:** backtest a trading strategy against SIP trade stream for the last five years in 10 minutes.

* **Vector embedding analytics:** easily add new input readers in Python.

* **Approximate quantiles for 10000 columns:** easily integrate with Arrow-compatible C++ Plugins.

* **TPC-H:** Several times faster than SparkSQL in many TPC-H queries. (EMR, not DBR!)

<p align="center">
  <img src="https://github.com/marsupialtail/quokka/blob/master/docs/docs/tpch-parquet.svg?raw=true" alt="Title"/>
</p>

## What is Quokka?

In technical terms, Quokka is a push-based distributed query engine with lineage-based fault tolerance.

In practical terms, Quokka is a tool for you to run custom stateful and windowed computation over terabytes of historical time series data.

Inspired by recent high performance database designs at Snowflake, DuckDB and SingleStore etc., it is meant to be much more performant than blocking-shuffle based alternatives like SparkSQL. On test TPC-H queries, Quokka currently is often several times faster than open-source SparkSQL and an order of magnitude faster than Dask. 

Unlike most other database engines, Quokka is implemented completely in Python and is meant to be easily extensible for new operations and use cases, e.g. time series analytics and feature engineering. 

Quokka operates on DataStreams, which are basically Spark RDDs except data partitions can be produced serially. A data partition can be consumed immediately after it's produced, unlike Spark where all the partitions have to be present in the RDD before the shuffle happens. This allows Quokka to pipeline multiple shuffles and I/O, leading to large performance gains.

## Install Quokka

Quokka requires Redis > 6.2. You can install latest Redis using: 

~~~bash
curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/redis.list
sudo apt-get update
sudo apt-get install redis
~~~

Make sure to run `redis-server -v` to check the Redis Server version! The default `apt-get install` version is most likely wrong.

~~~python
pip3 install pyquokka
~~~
**Docs**: https://marsupialtail.github.io/quokka/

Quokka offers a DataStream API that resembles Spark's DataFrame API:

~~~python
>>> lineitem = qc.read_parquet(s3_path_parquet + "lineitem.parquet/*")
>>> orders = qc.read_parquet(s3_path_parquet + "orders.parquet/*")
# filter with dataframe syntax
>>> d = lineitem.filter(lineitem["l_commitdate"] < lineitem["l_receiptdate"])
>>> d = orders.join(d, left_on="o_orderkey", right_on="l_orderkey", how = "semi")
# filter with SQL syntax
>>> d = d.filter_sql("o_orderdate >= date '1993-07-01'")
>>> d = d.select(["o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority"])
>>> d.explain() # this will produce a PDF execution graph you can visualize 
>>> d = d.compute()
>>> dataset = d.to_ray_dataset()
~~~

Currently Quokka supports reading data from CSV/Parquet on disk/S3 and Apache Iceberg through Glue, though theoretically any data source can be supported: Delta Lake/Hudi, S3 bucket of images, Ethereum blockchain through web3 APIs, transactional database CDC endpoints etc. If you have some esoteric data source that you want to run analytics on, please send me a challenge as a Github issue. 

## Fineprint

Quokka should not be used as a replacement for SparkSQL (it doesn't parse SQL directly yet, though it is on the roadmap). Instead you can play with it to see if it can give you better performance for your use cases. Another strength of Quokka is that it's Python-native, so you will never have to worry about JVM errors when you start using hairy UDFs with custom Python packages.

Quokka stands on the shoulders of giants. It uses [Ray](https://github.com/ray-project/ray) for task scheduling, Redis for lineage logging, [Polars](https://github.com/pola-rs/polars) and DuckDB for relational algebra kernels and [Apache Arrow](https://github.com/apache/arrow) for I/O. All of those are backed by efficient C++/Rust implementations. Quokka is a high-performance way of piecing them all together.

Please refer to the [docs](https://marsupialtail.github.io/quokka/) and examples in the apps directory. 

For any questions/concerns/just want to chat: zihengw@stanford.edu, or join the [Discord](https://discord.gg/6ujVV9HAg3) channel. Please do reach out before you use Quokka for anything real -- the code is currently written by a single underpaid PhD student, who is nevertheless happy to make Quokka work for your very specific use case.

Please raise a Github issue if you encounter any issues.
