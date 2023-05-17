
# Vector data lakes: why dataframes need to understand vector embeddings

This is a post preceding a upcoming talk at the [Data+AI Summit](https://www.databricks.com/dataaisummit/) this June. I will mostly use my dataframe library Quokka as an example, but the points made apply to other libraries too, like Spark, Pandas or Polars.

## Vector embeddings are here to stay

Vector embeddings are here to stay. It is hard to conceive of constructing recommendation or search systems in this day and age without using vector embeddings and approximate/exact nearest neighbor search. Vector embeddings are the easiest way to do analytics on unstructured data formats like text and images, and there are countless ways to generate them that get better each day.

## We need better ways of working with vector embeddings

It's clear from the get-go that vector embeddings are a whole new data type, and singificant changes to current data systems are needed to support them well. Hundreds of millions of VC dollars have poured into making a new generation of databases that are optimized around vectors. Existing SQL/noSQL players like Redis, Elastic, Postgres, Clickhouse, DuckDB have all built extensions that support vector operations. 

It is an open question which approach will win in the end -- a new data system with vectors at the core or a strong existing player with reasonable vector support.

I have my opinions on this issue, but that is not the topic of this blog post. **The topic at hand, is why are current dataframe systems so bad at handling vector data?** It used to be that dataframes lead databases in features (Python UDFs). However, in the case of vector embeddings, I believe they are falling behind.

For starters, there's no agreement on what the **type** of vector embeddings should be as a column in a dataframe. In Pandas, it will likely be an "object" datatype, which is opaque and unamenable to optimizations. Apache Arrow probably has the best idea, representing the vector embeddings as a FixedSizeList datatype. Unfortunately most people use Polars to operate on Arrow data, and Polars does not support FixedSizeList, only List, though there is an ongoing draft [PR](https://github.com/pola-rs/polars/pull/8342) to address this. In Spark we probably will use the ArrayType. Concretely this also means that Parquet files written by some systems will be unreadable by others.

**Wouldn't it be nice if there's a standard vector embedding data type** that every system understands? That data type needs to make its way into the storage standard (Parquet) and the in-memory standard (Arrow). Perhaps a group of important people can get together in a room and decide it, I don't know, but make it happen!

 Once the storage/memory type is settled, we should allow **dataframe-native** computations on the vector embedding column. Most people currently just do **.to_numpy()** on that column from the dataframe and start using ad-hoc numpy/faiss code. Then the resulting numpy array is stitched with other metadata back into a dataframe to continue processing in the relational world. 

Maybe most people think it's okay because that's the only option today, but I think it *sucks*. Imagine having to convert a numerical column to numpy every time you want to do a filter operation. At what point do you ditch the dataframe library altogether and start doing everything in numpy? Of course, **.to_numpy()** only works on single-machine libraries like Polars and Pandas. If you are using Spark, good luck. Maybe write a UDF or something? 

I think dataframes should support native operations on vector embedding columns, such as exact/approximate nearest neighbor searchs or range searches. But wait -- don't you need an index to get any semblance of good performance? Well, Pandas already has an index, so perhaps it's not too hard to add another one. Polars famously does not have indexes, but the index can be stored as a separate structure in memory. Finally, is exact nearest neighbor search really so bad? With GPUs that can do a few trillion FLOPs per second available at $1 per hour on AWS, maybe not. 

## What Quokka is doing

As a proof of concept and hopefully example for other dataframe systems, I have started implementing vector-embedding-native operations in Quokka. For those unfamiliar, Quokka is a **distributed** dataframe system currently largely supporting the Polars API, with an aspiring [SQL](https://github.com/marsupialtail/quokka/blob/master/pyquokka/sql.py) interface. It is fault tolerant and usually much faster than SparkSQL if data fits in memory. You can also use Quokka locally, just do `pip3 install pyquokka` and familiarize yourself with the API [here](https://marsupialtail.github.io/quokka/simple/). Similar to Spark and Polars, Quokka has a lazy API so it can optimize a user's query before running it by pushing down filters, selecting columns early and reordering joins.

**IO**: Quokka supports ingest from the [Lance](https://github.com/eto-ai/lance) format. Lance is an on-disk alternative to Parquet specifically optimized for vector embedding data with an optional PQ-IVF index built on the vector embedding column. If you are working with vector embedding data, you should strongly consider using Lance. It is still lacking integrations to Delta Lake and a few other features, but its team includes a Pandas co-founder and delta-rs contributor, so its future is bright.

To read a Lance dataset into a Quokka DataStream, just do `qc.read_lance("vec_data.lance")`. You can also read many Lance datasets on object storage into the same Quokka DataStream: `qc.read_lance("s3://lance_data/*")`. 

**Compute**: Quokka currently supports just one operation `vector_nn_join` on vector embedding data, with plans to add one more `vector_range_join` if people are interested.

## What I hope this enables: open vector data lakes

Let me spend a few moments here to describe what I hope adding vector embedding support to a distributed dataframe library allows people to do.

In the structured data world, *data lakes* have become popular as a long-term storage for OLTP stores like Postgres or MySQL. The data lake has much worse transactional performance for online workloads, but support cheap long term storage and relatively efficient querying with *lakehouse* tools like Trino, SparkSQL, Databricks Runtime, or Snowflake Iceberg. Most importantly, the long term storage is in an **open format** decoupled from the OLTP store, allowing different tools to compete and excel at different tasks, like dashboard reporting or machine learning training. Can you imagine a world where company A's object store is packed with MySQL pages and company B's Postgres WALs?

Vector databases today much resemble the OLTP databases with strong focus on ingest speed, write consistency and point query latency. However, when their size starts to blow up, so does their cost. Vector database A's long term storage format on object store is effectively closed to all analytics tools other than vector database A. Do you really want to lock up your vector embeddings in this way?

Well, the answer is *it depends*. If your workload is such that you need sub-second latencies to all of your vectors, and you are only ever doing single or chained point updates or lookups, then perhaps this *is* the best solution for you. Pick the best vector database and eat the cost.

However, if a big part of your workload on vector embeddings resembles "classic OLAP" where you can tolerate higher latency, e.g. updating cached recommendations every hour or offline analysis of your embeddings, there is strong reasons to believe you should move your vectors out of that database into a **vector data lake**.

What does this **vector data lake** look like? Vector embeddings should be stored in Parquet, or Lance, as a native data type. Data lake formats such as Delta Lake or Iceberg should allow a place to stick the ANN index anyone might want to build, and support versioning on these indexes. Query engines such as Trino and SparkSQL should be able to do nearest neighbor search on the vector data, just like how they are able to filter or join relational data.

Of course, vector databases are still needed to provide operational access to the latest data, just like Oracle/Postgres/MySQL. However, old data should be periodically ETL'ed out of those systems to the data lake. Ask your data engineering team. They know the drill.

Quokka is the first system that allows people to 
