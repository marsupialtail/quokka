<p align="center">
  <img src="https://github.com/marsupialtail/quokka/blob/master/docs/quokka-banner.png?raw=true" alt="Title"/>
</p>

Docs: https://marsupialtail.github.io/quokka/

Quokka is a pure-Python fast data processing engine. It can be leveraged to obtain near-peak performance on SQL queries on data "lakes" with CSV and Parquet file formats. It is often several times faster than SparkSQL and an order of magnitude faster than Dask. 

Quokka stands on the shoulders of giants. It uses [Ray](https://github.com/ray-project/ray) for task scheduling, Redis for lineage logging, [Polars](https://github.com/pola-rs/polars) for relational algebra kernels and [Apache Arrow](https://github.com/apache/arrow) for I/O. All of those are backed by efficient C++/Rust implementations. Quokka is a high-performance way of piecing them all together.

Quokka offers a DataStream API that resembles Spark's DataFrame API. Currently it supports reading data from CSV/Parquet on disk/S3, though theoretically any data source can be supported: Apache Iceberg/DeltaLake/Hudi, S3 bucket of images (apps/pinecone.py), Ethereum blockchain through web3 APIs, transactional database CDC endpoints etc. If you have some esoteric data source that you want to run analytics on, please send me a challenge as a Github issue. 

Quokka should not be used as a replacement for SparkSQL (it doesn't parse SQL directly yet, though it is on the roadmap). Instead you can play with it to see if it can give you better performance for your use cases. Another strength of Quokka is that it's Python-native, so you will never have to worry about JVM errors when you start using hairy UDFs with custom Python packages.

Please refer to the [docs](https://marsupialtail.github.io/quokka/) and examples in the apps directory. 

For any questions/concerns/just want to chat: zihengw@stanford.edu, or join the [Discord](https://discord.gg/YKbK2TVk) channel.

Please raise a Github issue if you encounter any issues!
