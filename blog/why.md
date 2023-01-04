# Why I wrote a SQL engine in (only) Python
I've worked on Quokka for a year now. Before I started, I was on leave from Stanford working on a startup writing assembly speeding up machine learning primitives like [matrix multiplications](https://ziheng-4209.medium.com/how-to-do-fast-sparse-int8-multiplications-on-cpus-to-speed-up-deep-learning-inference-bb64f4bf8a35) and [decision trees](https://ziheng-4209.medium.com/extremely-fast-and-cheap-decision-trees-67f2750b1ab3). After a while, I realized though I could perhaps make a living doing the above, what most customers want sped up is not ML model inference/training but data pipelines. After all, most ML in industry today seems to be lightweight models applied to heavily engineered features, not GPT3 on Common Crawl. 

Virtually all feature engineering today is done with SQL/DataFrames with a prized library of user-defined-functions (UDFs) that encode business logic. Think fraud detection, search recommendation, personalization pipelines. In model training, materializing those features is often **the** bottleneck, especially if the actual model used is *not* a neural network. People now either use a managed feature platform like [Tecton](https://github.com/feast-dev/feast), [Feathr.ai](https://github.com/feathr-ai/feathr) or roll their own pipelines with SparkSQL. With robust UDF support, SparkSQL seems to be the defacto standard for these feature engineering workloads, and is used under the hood by virually every managed feature platform. (Unless you are on GCP, in which case BigQuery is also a strong contender.)

Of course, these problems only happen when you have big data (> 100GB), can't use Pandas, and need to use a distributed framework like SparkSQL. Having lost all the money I made from my startup on shitcoins and the stock market, I returned to my PhD program to build a better distributed query engine, Quokka, to speed up those feature engineering workloads. When I set out, I had several objectives:
* **Easy to install and run**, especially for distributed deployments. 
* Good support for **Python UDFs** which might involve numpy, sklearn or even Pytorch. 
* At least 2x SparkSQL **performance**. Otherwise what are we even doing here.
* **Fault tolerant**. This is perhaps not important for small scale frameworks, but it's critical for TB-scale jobs that run over many hours that are a pain-in-the-ass to restart. SparkSQL can recover from worker failures due to spot instance pre-emptions, Quokka should be as well. 

The first two objectives strongly scream **Python** as the language of choice for Quokka. PySpark supports Python UDFs reasonably well, but there are a myriad of inconveniences that arise from its Java background -- you have to sudo install all required Python packages on EMR, Python UDF error messages will not get displayed properly, no fine-grained control of intra-UDF parallelism etc. While each issue seems minor on its own, these footguns are extremely annoying for Python-native data scientists like me whose last experience with Java was AP Computer Science. 

I had pushback on this -- I know major tech players who maintain UDF libraries in Scala/Java, and senior engineers who claim Java is not so bad and all serious engineers should know it anyways. My argument:
* I got a CS degree at MIT without writing a single line of Java. I know many who did the same. 
* I want to empower data scientists without a formal CS education, and it's unlikely their first language of choice is Java based on the number of tutorial videos available on YouTube. 
* Ever wondered why Tensorflow4j exists? Do you even want to learn how to use it instead of just writing PyTorch? 

But how do you build a distributed engine on top of Python? After all Python is not known for its distributed prowess... Until [Ray](https://github.com/ray-project/ray) came about. Not going to waste space here describing how amazing it is -- but it's basically Akka in Python that actually works. It allows you to easily instantiate a custom Python class object on a remote worker machine and call its functions, which is pretty much all you need to build a distributed query engine. Ray also let's you easily spin up remote clusters with a few lines of code and manage arbitrary Python dependencies programmatically, which easily satisfied my first two objectives.

Well now, **what about performance**? Python is so reputed for being slow there are [memes](https://www.pinterest.com/pin/why-python-is-popular-despite-being-super-slow--615867317773696019/) for it. However, Python's slowness actually works in its favor -- since it's so slow, people have built amazing open-source libraries for it in C or Rust that speeds up common operations, like numpy, Pandas, or Polars! If you use those libraries as much as possible, then your code can actually be extremely performant: e.g. if you implement a data analytics workflow using only columnar Pandas APIs, it will beat a handcoded Java or even C program almost any day.

Specifically, for a distributed query engine, you want:
* A library to parse and optimize SQL, the one and only [SQLGlot](https://github.com/tobymao/sqlglot). It's a SQL parser, optimizer, planner, and transpiler, and can even execute simple SQL queries in pure Python.
* Very fast kernels for SQL primitives like joins, filtering and aggregations. Quokka uses [Polars](https://github.com/pola-rs/polars) to implement these. (I sponsor Polars on Github and you should too.) I am also exploring DuckDB, but I have found Polars to be faster so far. 
* Fast reading/ writing/ decoding/ encoding CSVs and Parquet files. Quokka uses [Apache Arrow](https://github.com/apache/arrow), which is probably the fastest open-source option, and will become even faster when my [PR](https://github.com/apache/arrow/pull/14269) gets merged! 
* A tool to send data quickly from one worker to another without having to serialize. Ray's object store provides a built-in solution, but Quokka opts for the higher-performance [Arrow Flight](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/).

In database parlance, Quokka adopts a pipelined approach where multiple processing stages can happen at once, similar to [Presto](https://github.com/trinodb/trino) or Snowflake. For example, if your workload involves reading from two data sources in S3 and joining them, Quokka can execute the download, decode and join all at once. This offers higher performance than SparkSQL's model, where stages execute one at a time -- Spark would download the data sources first, and only after they are fully buffered in memory or on disk would it execute the join stage. 

Of course, the pipelined execution model leads to complications with fault handling. In Spark fault tolerance is pretty simple -- if a stage fails it just gets re-executed. Only one stage could fail at a time. In Quokka, multiple stages can fail at once, and it's hard to see how you can recover from failures without just restarting the whole damned thing. 

Efficient fault handling in pipelined query execution is actually the main academic innovation of Quokka and how I hope to earn my PhD -- so it's perhaps best to save it for another post. But a sneak peak -- similar to how Kafka writes control information to Zookeper or how Kubernetes manages control plane with etcd, Quokka maintains a consistent global state in Redis. This allows Quokka to reason about failures and recover from them using dynamically tracked lineage. 

**OK enough talk! What's the state of Quokka right now, and can I use it?** 

Yes, of course. Quokka currently supports a DataFrame-like API, documented [here](https://marsupialtail.github.io/quokka/simple/). It should work on local machine no problem (and should be a lot faster than Pandas!). Distributed setup is a bit more [involved](https://marsupialtail.github.io/quokka/cloud/) and only supports EC2 right now. 

SQL support is in the works and currently passes half of the TPC-H benchmark. For a look at how to implement these queries in the DataFrame API, check [here](https://github.com/marsupialtail/quokka/blob/master/apps/tpc-h/tpch.py). Quokka's performance is similar to Trino (who is not fault tolerant) at the moment on these queries for Parquet and a lot faster than everybody else if the input is in CSV format. 

<p align="center">
  <img src="https://github.com/marsupialtail/quokka/blob/master/docs/docs/quokka-4-csv.svg?raw=true" alt="Title"/>
</p>

Quokka is under active development. In the new year, I hope to add a lot more functionality related to feature engineering, i.e. range joins, PIT joins, window functions etc. as well as improve upon the SQL execution engine's performance, potentially using SIMD kernels. I will also add some more data sources, like the Twitter API, Ethereum API, JSON files, and probably JDBC. Finally I plan to add support for connecting to an existing Ray/Kubernetes cluster, and GCP/Azure.

Quokka's core execution engine is only 1000 lines of code. It is meant to be simple and easily extensible. I welcome any contributions on new data source [readers](https://github.com/marsupialtail/quokka/blob/master/pyquokka/dataset.py) or [executors](https://github.com/marsupialtail/quokka/blob/master/pyquokka/executors.py)! 

If you think Quokka's cool, please join the [Discord](https://discord.gg/6ujVV9HAg3), raise a Github issue or shoot me an email: zihengw@stanford.edu.
