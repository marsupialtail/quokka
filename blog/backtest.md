# Tick-level trading simulation in the cloud
This blog post showcases a prime use case of Quokka's time series functionality -- backtesting a mid-high frequency trading strategy against the SIP trade stream on US equities. There are a couple different ways to backtest strategies (if you are a professional feel free to skip to the next section):
 
- **Bar backtesting**: A common strategy used by retail investors is to use candlesticks (i.e. bars), and assume you enter a position at the open/close of a bar and exit the position at the open/close of some subsequent bar. 

	There are obvious downsides to the bar-level backtesting strategy, the most important of which is the bucketization of time. For example if you use 5-minute bars, you cannot enter the market until the end of the interval. This means if you happened to want to trade at 9:33, you have to enter the market at 9:35 if you are using 5-min bars. Your alpha might have decayed by then.
	
- **Trade backtesting**: Another strategy used by more professional investors involves looking at the list of trades that happened on the exchange to figure out what the entry price would have been for their proposed historical trades. The simplest way to do this is to find the next trade that happened right after your proposed trade, and use that trade's price as the simulated execution price. You can also use more complicated techniques like VWAP.

- **Book backtesting**: Even trade-level simulation can be improved upon by using order book information. What if you wanted to trade one million shares, and the flanking trades of your proposed trade at that time only had size 100? Using the execution prices of those trades will not accurately reflect the market impact of your trade, and you will most likely get a much worse execution price. The most accurate way to backtest would be to reconstruct the order book at the time of your proposed trade and compute exactly your execution across multiple levels.  However, the book information is very expensive and can easily amount to hundreds of GB per day. You will also have to make assumptions like the position of your trade on the exchange queue, which might be suitable only for HFT players.

This post will focus on using Quokka to perform **trade-level backtesting**. It strikes a good balance between bar-level simulation and book-level simulation in terms of accuracy and speed. Quokka can perform the other two types of simulation as well, which we will cover in the future.

## Why cloud?

One common way to do trade-level simulation is to fire up a huge box with fast NVMe SSDs, shove all the trade ticks in there (on the order of tens of billions of events), and use a solution like KDB or fast event loops in C++ to run through these ticks every time you want to do a simulation.

This approach quickly runs into problems in terms of scalability. If your company has multiple quants, they are for sure going to be fighting over access to this box. Even if you just have one quant, he/she can't really test strategies in a scalable way as soon as the disk throughput is saturated on the box.

<p align="center">
  <img src="https://github.com/marsupialtail/quokka/blob/master/docs/docs/backtesting.svg?raw=true" width="400" height="400">
</p>

At this point, you might want to start adding more boxes, and start assigning people to different boxes. Expenses aside, this data duplication introduces a nightmare of its own -- you will have to upload new market data to each of the boxes, making sure they are consistent. Oh and what happens when you want to backfill some correction on your data, e.g. changing the way the data is stored or maybe stock splits? I've been there. It's not a good place to be. 

The alternative solution, is just to store **one single source of truth** of your market data in a shared location, i.e. in Parquet files in an S3 bucket.  This is much cheaper than trying to copy it onto different NVMe SSDs, automatically resolves consistency issues and is generally much simpler to manage. The only downside might be you can't run backtests when you are updating or backfilling this data, but you probably shouldn't be anyway.

Whenever we want to run backtests, we spin up ephemeral resources like EC2 spot instances or Kubernetes pods to pull the data and crunch it. This is very appealing because we can scale up easily and request new resources for each quant or strategy, easily enabling hundreds to thousands of backtests in parallel. We can even parallelize within a single backtest on different machines across different tickers or time ranges. The only downside is that we are reading the same data over the network many, many times. However this is fine because IO within the same AWS region is free and has become fast enough to be competitive with local NVMe SSD bandwidth!

The added benefit of this is that you can use a managed cloud data platform like Databricks, which other parts of your organization (i.e. sell side quant/risk) might already have contracts with. These companies are also where the brightest software engineers tend to be at the moment, so it's probably a good bet. 

## Quokka OrderedStreams

The question that is now probably hovering in your head is **how fast can you run backtests in this way**? I will show you that you can perform a trade level simulation with eight machines on all the trades in the SIP feed (NASDAQ-A,B,C) across four years (2019-2022) in 10 minutes with Quokka. Why 10 minutes? This is the typical time it takes to go down the elevator, get a coffee and come back in NYC. 

Some parameters for this backtest: we will use the SIP tape from 2019-2022, containing tens of billions of trades. We filter out all the trades that occurred outside of market hours and not on the NASDAQ tapes (primarily FINRA reports which we likely can't execute on). We test a strategy that makes one million random market orders in that time horizon on 1000 stocks, with each trade exiting five hours later or at next market open.

Quokka offers a distributed **OrderedStream** abstraction, which represents an ordered stream of data batches, potentially partitioned and parallelized by an attribute. We represent our SIP trade stream as an OrderedStream ordered on timestamp and partitioned by ticker. We can store the trade stream itself in a series of Parquet files in an S3 bucket. Quokka automatically infers the sort order amongst the files using Parquet metadata:

~~~python
>>> qc = QuokkaContext(cluster) 
>>> df = qc.read_sorted_parquet("s3://nasdaq-tick-data/yearly-trades-parquet-all/*",
	sorted_by = "timestamp")
~~~

We can easily filter this stream with SQL or DataFrame syntax and create new columns:
~~~python
# filter for trades on NASDAQ tapes only and remove exchange open/close events
>>> df = df.filter_sql("exchange in ('T', 'Q') and condition not in ('16','20')")
# make a human readable timestamp
>>> df = df.with_columns({"timestamp_hr": lambda  x: polars.from_epoch(x["timestamp"] - 5 * 3600 * 1000, time_unit = "ms")})
# extract the hour and minute from the human readable timestamp
>>> df = df.with_columns({"hour": lambda  x: x["timestamp_hr"].dt.hour(), "minute": lambda  x: x["timestamp_hr"].dt.minute()})
# filter for trades that happened only during the trading day
>>> df = df.with_columns({"minutes_in_day": lambda  x: x["hour"] * 60 + x["minute"]})
>>> df = df.filter_sql("minutes_in_day >= 570 and minutes_in_day <= 960")
# show the schema of our OrderedStream
>>> print(df)
OrderedStream[timestamp,price,volume,exchange,condition,symbol,date,timestamp_hr,hour,minute,minutes_in_day] order by {'timestamp': 'stride'}
~~~ 

Quokka contains a variety of database optimizations which make filters, map functions and column selections extremely efficient.

We can now specify a stateful operator to apply to this OrderedStream to perform our backtest. Quokka ensures that this stateful operator is applied sequentially on the OrderedStream according to the specified order key, which is timestamp. Quokka also allows us to specify a partition key like stock ticker to parallelize this process on different partitions.

~~~python
>>> executor = Backtester(alpha)
>>> df = df.stateful_transform(executor, new_schema = ["date", "equity"], required_columns = {"date", "timestamp", "symbol", "price"}, by = "symbol")
>>> df.explain()
>>> results = df.collect()
~~~

`df.explain()` will show the Quokka logical execution plan as an image, and is extremely helpful in understanding what Quokka actually does under the hood. Finally, everything we have done up to this point only lazily transforms the OrderedStream `df`. We need to call `collect()` on it to get the actual results.

The `Backtester` can be any custom Python class that exposes two methods: `execute`, what to do when it sees a new batch, and `done`, what to do when no more batches will come. The Backtester is free to use whatever Python libraries it sees fit. An example implementation which attempts to match each submitted order to the next occurring trade in the SIP stream is shown [here](https://github.com/marsupialtail/quokka/blob/master/apps/rottnest/backtester.py). 

All the code that I have shown thus far would execute locally on a client, i.e. your laptop. When you call `collect()`, Quokka will start dispatching tasks to a distributed cluster. Eventually the results are collected back as a Polars Dataframe onto the client. You can then manipulate the results with Polars and plot the results using your favorite Python library.

~~~python
>>> pnl = results.groupby("date").agg(polars.col("equity").sum()).sort("date")["equity"].to_list()
>>> plt.plot(pnl)
>>> plt.show()
~~~

## Parting Words

The full end to end code for this example can be found [here](https://github.com/marsupialtail/quokka/blob/master/apps/rottnest/backtester.py). It goes through the 400 GB of compressed Parquet format SIP trade stream on S3 in around 10 minutes with eight r6id.2xlarge instances on AWS, which costs around $0.4 assuming we use spot instances. If we run 1000 backtests per month this would be only $400. The cost to store this data on S3 is negligible -- only $10 a month. 

Most importantly, we have achieved **storage-compute separation**. You are storing a single copy of the data, and spin up as much compute on demand as you need to process potentially unlimited backtests in parallel. Gone are the Friday evening queues to get your backtest job in to the big box with ticks loaded on disk!

Why not just roll your own stack based on this architecture, write your own code to download stuff from S3 and go through everything with an event loop? Quokka is indeed based on this architecture, except with heavy performance optimizations and a multitude of convenience utilities, e.g. filtering an OrderedStream with SQL. Quokka performs complex database operations like predicate pushdown to make sure it's reading as little data as possible to save you money and time.

You can always use the parts of Quokka you want (e.g. Parquet loading from S3, filtering etc.), and roll your own Cython or C++ code for the actual computation. For example, the Backtester class listed above could call out to any custom C++ plugins, e.g. to efficiently build the order book.

In the coming months, I will write a blog post on how to use Quokka to actually quickly test complex trading rules to come up with the alpha signals, e.g. buy a stock every time when certain trades of a certain type happened in quick succession. Stay tuned.
