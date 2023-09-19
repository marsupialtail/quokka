# Making Data Lakes Work for Time Series

I have been working on Quokka for almost two years now. It started as a generic query engine, or a Python version of SparkSQL. When I started out, I didn't really have a specific application in mind, except maybe publish a paper to further my PhD. Having failed at that (repeatedly), I am happy to announce that I've settled on a calling for Quokka -- making data lakes work for observability. 

Observability = time series data like metrics, logs and traces typically used for debugging or application performance monitoring. Quokka is now 100% focused on solving what I believe is the core issue in observability data management today -- **building a unified PB-scale data store for time series data on object storage**.

## The Problem

**Why is such a data store needed, and why does it have to be on object storage like S3?** It is well known that observability is a data problem. A lot of observability runs well on a very small set of recent metrics and important log sources. Open source solutions like Prometheus, Elastic/OpenSearch and vendor solutions like Splunk and Datadog excel at interactive analytics on such "hot" data.

However, inevitably you find yourself having to look past hot data at older logs/metrics or logs from services you didn't think were important. Typically this happens at 2am at the end of a long debugging session when the logs you can afford to shove into ElasticSearch couldn't solve your problem. 

**The ideal solution is to store everything, all the time.** However, once you start doing that, you start hitting hard limitations with current open source solutions and $100K+ bills from vendor solutions.  For example, Grafana Loki cannot handle [high cardinality data](https://github.com/grafana/loki/issues/91) because it relies on partitioned tags for full text search. Elastic/OpenSearch doesn't have this problem, but you might need a PhD to manage it properly. The large [sizes](https://discuss.elastic.co/t/why-is-my-elasticsearch-index-using-so-much-disk-space/202117) of ElasticSearch indexes also become a problem, even in cold storage. What's worse, scaling solutions for metrics, logs and traces tend to be disparate (Thanos vs. Loki. vs. Elastic vs. Tempo), which leads to a bad cross-correlation experience and high maintenance cost. You can make all these problems disappear by handing Datadog millions of dollars a year, but not everybody can spend like Coinbase in 2021.

Increasingly, organizations have started to put old data in Parquet files in Delta Lake or Iceberg with Spark or Presto as the querying tool. This puts all observability data in one place and facilitates easy cross correlations between different data sources. The problem? Data lakes are are designed for full table scans and aggregations, not the needle-in-the-haystack queries common in observability, like filtering by UIUD. As a result, naively using a data lake leads to very cheap data storage, but extremely slow or expensive querying. Hosted solutions like AWS Athena can take tens of minutes when you try to full text search on multiple terabytes of logs. 
 
 ## The Solution

**Can we make data lakes better at observability queries?** Definitely. One approach is to invent some new format, like InfluxData's [IOx](https://github.com/influxdata/influxdb_iox). However, I am a strong believer in building off existing platforms such as Delta Lake and Apache Iceberg. You get nice things like ACID and metastore for free, as well as automatic SQL support from query engines like Databricks, Snowflake or Trino. 

Now let's face the elephant in the room: Delta Lake/Iceberg as they are cannot do full text search. Nor are they particularly efficient at high cardinality time series retrieval. However, just like how virtually every single-node database (Postgres, DuckDB, SQLite) have extensions to help them do difficult things, it should be possible to **build such data lakes extensions** too!

What does a data lake "extension" look like? Well it is just an external index, similar to regular database extensions. Except this external index has to live on object storage and accessed as such, similar to the "internal index" data lakes currently have in the form of Parquet stats and metadata files. There are a few nice properties of this external index that we should strive for:

- **Small**: ideally size is a fraction of the original Parquet data, which already perform serious compression.
- **Fast to compute**: only a handful of servers should be needed to keep the index up to rate, even at TB/hr ingestion rates. On-demand indexing should also be supported, even for data far in the past.
-  **Efficient random access**: if the index is tiny, like Parquet footers, it maybe feasible to download/cache all the index files upon query. However, if the index is larger, it must support efficient random access to avoid full download on every query.
- **Compatibility**: it goes without saying that the index should not modify the underlying data lake such that readers who have no idea that the index existed can still read the data.

Armed with this external index, here is a diagram of the reimagined observability workflow:

<p align="center">
  <img src="https://github.com/marsupialtail/quokka/blob/master/blog/rottnest.svg?raw=true" >
</p>

Applications send important hot data to monitoring services like ELK stack, Grafana Prom/Loki/Tempo (PLT) stack, or vendored solutions like Datadog or Splunk. They also send a copy of this hot data, as well as all other data that *could* be useful to a data lake, such as Delta or Iceberg. Data in the data lake is lazily indexed by an autoscalable indexing service. When the SRE debugs issues, she goes first to the hot data in ELK/Datadog. Most issues should be able to be debugged this way. However, inevitably, for some issues, the SRE needs to query older data or sources not in the hot data service, she then goes to query the data lake. Armed with this external index, query engines like SparkSQL/Presto/Quokka can efficiently perform full text search. With a good enough index, the SRE can even perform such queries locally from her laptop!

This architecture lets you do two things: 
-  **Save cost**: significantly reduce the required retention in your hot data stack while maintaining searchability.
-  **Reduce MTTR**: afford to store all logs and search things you otherwise would simply have thrown away.

The above diagram illustrates the role of Quokka in the process. **First**, Quokka will enable the computation of these indexes. Most of the work has already been done and will be gradually open sourced in the following months following some real test deployments. **Second**, Quokka will serve as an example query engine that can speed up observability queries with these indices. Over the past few months, I have been hard at work adding Splunk/EQL syntax into Quokka such as `transactions` and `sequence`, which will be open sourced as well.

Trino/SparkSQL/DuckDB will most likely be supported through SQL-level rewrites, where the index is first queried to add a very strict timestamp filter on the original query. For example, if you search for `select * from logs where block_id = 'blk_123456'`, the full text search index will first be queried for `blk_123456`, which will return a timestamp filter like `timestamp between 2020-09-08 and 2020-09-09`. This filter is added to the original query. While the original query might involve a full table scan on the `block_id` string column, the new query can probably be run interactively with DuckDB on the SRE's laptop.

A couple things of note:

- **The data lake works alongside hot data monitoring.** If you instantaneously index all incoming data, queries on ELK/Datadog should theoretically be possible on the data lake as well. However, there is still a big gap between the usability of SQL-based observability tools and purpose-made tools like ELK/Datadog. The SRE might simply prefer the Kibana or Datadog interface, which is perfectly reasonable. 
- **Index/Search Decoupling**. In a key difference from Elasticsearch, this approach naturally decouples searching with indexing. Log ingestion and log search are typically both bursty processes, though unfortunately the bursts tend to happen at different times: the former peaks when new code is deployed and the latter peaks when the bespoke code breaks, which can be many days later!

**Preliminary results** show that the size of the index needed to support full text search is around 1% of the uncompressed logs size, and 20% of the Parquet file size. This means that 1TB of logs, which costs upwards of $2500 for 30-day retention on some vendor platforms, turns into 40GB of Parquet and 8GB of index, costing only around **$2** in this indexed data lake, including indexing costs. Most full text queries based on UIUDs can be done interactively locally from your laptop. If you are interested in a real-life demo on your own logs, email me at zihengw@stanford.edu

## Summary

Over the past few years, I have noticed time-series databases have each rolled their own scalable storage layer (Promtheus-Mimir, Loki, Elastic Frozen Tier, InfluxDB-IOx). This new direction for Quokka aims to answer the question of whether or not they can instead just use widely adopted data lake formats like Delta Lake and Iceberg, albeit with minor extensions.

Ultimately, if this approach takes off, I see vendors selling their own "managed extension services", each promising to do some magical things with your data. Perhaps  I will be one of them. While this could certainly introduce problems with extension compatibility issues down the line, it is infinitely better (in my opinion) than the state of the world right now where each data type is locked in its own data format, leading to issues like [this](https://github.com/thanos-io/thanos/issues/2682). 

While this discussion has largely focused on observability, data lake extensions that support efficient time-series retrieval and text search could be very useful for other fields as well, such as financial data management, LLM training data curation, tag-based image retrieval etc.

Over the next few months, I will release API specifications for managing these extensions and  code demos, after I test out this approach in some real deployments. In the meantime, if you are interested, please join the [Discord](https://discord.gg/jksW97EH) or just email me at zihengw@stanford.edu. Love to chat about anything, especially furry animals with tails.
