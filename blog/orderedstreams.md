# Quokka OrderedStreams  for PB-scale Time Series Analytics

**WORK IN PROGRESS, some APIs not in public release! Header will be changed on actual release date.**

Today I am happy to announce preview features for Quokka on its main focus, terabyte-scale cloud-native time series analytics on events, metrics and logs. These can be semi-structured text logs in csv or json format, or already-compressed metrics like ticker-level trade data in Parquet or numpy format. It takes heavy inspiration from existing systems like kdb+, Splunk and [Flint](https://github.com/twosigma/flint) but strives to go (much) above and beyond.

## What

Quokka allows you to ask questions of the following types: (financial data is used as example though it could easily be security or APM observability traces)

- Given terabytes of historical tick-level trades and quotes, do an `asof` join to quickly find the latest quote before a trade, or a range join to find a range of quotes before the trade.
- Given terabytes of quotes data, build an order book for each ticker with a custom order book implementation in Python or C++.
- Given the joined trades/quotes stream, perform complex event recognition like detecting all events where first predicate X is satisfied, then within A seconds predicate Y is satisfied, then within B seconds predicate Z is satisfied, etc. A, B, X, Y and Z can be user-defined functions. This can be used to easily build custom market microstructure features.
- Given terabytes of ticker-level data, perform interactive backtesting of custom strategies using hundreds of machines.

Importantly, Quokka's focus is on *immutable* historical data residing in a data lake. Quokka currently does not support consistent upserts like real databases, e.g. kdb+. Quokka expects upserts to happen at a bulk level inside a data lake management system like Apache Iceberg, which it supports as input source.

**Speed** is Quokka's main focus and my passion. Quokka tries to be limited by fundamental limits like network bandwidth or how fast you can parse Parquet (not really fundamental, new Parquet reader impls welcome) instead of its own implementation, which is based on [Polars](https://github.com/pola-rs/polars), [DuckDB](https://github.com/duckdb/duckdb) and its own C++ plugins library called *ldb* (what comes after k?).

**Generality** is Quokka's main distinguishing factor. Too many incoming quants, SREs or security analysts know only Python and SQL. Quokka meets them where they are at and allows efficient stateful computation over time series data in Python or SQL. They can always drop down to C++ or Rust to build custom operators similar to Tensorflow/PyTorch custom ops if speed is of the essence.

Finally, Quokka is **fault tolerant** to worker failures. This allows long running pipelines to use spot instances on the cloud to save cost, as well as Kubernetes style deployments where pods might get pre-empted.

## How

Quokka offers a Polars-like dataframe interface, aka Quokka DataStreams. Quokka DataStreams are different from Spark RDDs -- a DataStream is a collection of batches logically materialized in sequence instead of all at once. The DataStream supports an extensive list of Polars-like things, like `filter`, `select`, `with_columns`, `join` etc. Its incrementally materialized nature allows it to naturally handle workflows on sorted time-series datasets.

In Quokka, a sorted DataStream is called an **OrderedStream**. They have a sort attribute (e.g. time) and optionally a partition_by attribute (e.g. ticker ID or log tag). They can be created from data sources like a directory of Parquet files, Apache Iceberg, text logs with metadata, etc. For observability folks, Quokka is trying to add support for [Vector Remap Language](https://vector.dev/) as an input source.

```
>>> trades = qc.read_sorted_parquet("s3://market-data/ticks.parquet", sorted_by = "time")
>>> quotes = qc.read_sorted_parquet("s3://market-data/quotes.parquet", sorted_by = "time")
>>> quotes
>>> OrderedStream([time,symbol,seq,bid,ask,bsize,asize,is_nbbo], sort_by=time)
```

Importantly, like Spark DFs, OrderedStreams are lazy and only materialized when the user finally executes a blocking call like `collect()` or `write_parquet()`. 

OrderedStreams support operations such as `join_asof`, `join_range`, `windowed_transform`, `shift`, `pattern_recognize` and `ordered_stateful_transform` that are not supported by a regular unsorted DataStream.

To perform an `join_asof` to match quotes to trades and select the bsize/asize of the matching quote and the size of the subsequent trade:
```
>>> result = trades.join_asof(quotes, on = "time", by = "symbol")
>>> result.select([time,symbol,bsize,asize,size])
>>> OrderedStream([time,symbol,bsize,asize,size], sort_by=time, partition_by=symbol)
```
We can perform aggregations as we would do a regular DataStream:
```
>>> result.sum("asize")
```
Running this `asof` join followed by sum on four r6id.2xlarge worker machines on AWS against 1.3 billion quotes and 250 million trades on AWS took around 35 seconds with Quokka. Spark doesn't directly support `asof` joins, however, it takes 200 seconds with highly optimized SQL code in Spark that uses a union, sort and join similar to this [trick](https://gist.github.com/RMB-eQuant/758539f8914f2dd4461ec0ce144b048b). Quokka achieves **6x speedup** out of the box with this use case.

Quokka allows you to do much more than `asof` joins. For example you can write a custom Python class that exposes an `execute` handler to process each batch in the OrderedStream to do anything you want, like building an order book:
``` 
# Very simplified example, not production code!!
class OrderBook:
	def __init__(self):
		self.order_book = ...
	def execute(quotes):
	    for quote in quotes:
	        if quote["side"] == "bid":
	            self.order_book["bids"][quote["price"]] += quote["volume"]
	        else:
	            self.order_book["asks"][quote["price"]] += quote["volume"]
	def done(self):
	    return polars.from_dict(self.order_book)
```
Now transform the `quotes` OrderedStream with this class:
```
order_book = quotes.ordered_stateful_transform(OrderBook(), new_columns = ["bid","ask"])
```
You can write any custom Python class you'd like and do things like run backtests or trying out real-time ML training strategies. If performance becomes a problem, Quokka offers a way to use C/Rust extensions similar to PyTorch or Tensorflow custom ops.

## More

Quokka can do even more -- one of the things I always wished I was able to do at scale on time series data is complex event recognition, like this kind of trade happened, then within twenty seconds we see this kind of quote, then within thirty seconds we saw this other kind of trade. Or imagine you want to find patterns in your security logs of the type -- this person logged into machine X and within thirty seconds logged into 100 other machines and then logged off.

Implementing this kind of detection was always a pain in the a** in Pandas or Polars. Recently, SQL standard introduced the `MATCH_RECOGNIZE` pattern. However, it is only supported by a few vendors and don't directly support some key things that I want, like time duration conditions between observations inside an event.

Quokka OrderedStream's `pattern_recognize` pattern solves this problem, for myself and maybe for you:

```
>>> events = quotes.pattern_recognize(
	anchors = [
		quotes["asize"] > 1000,
		quotes["asize"] < 10,
		quotes["asize"] > 1000
	],
	duration_limits = [
		'10s',
		'20s'
	],
	partition_by = "symbol"
)
>>> events
OrderedStream([time,eventID,anchor1,anchor2,anchor3])
```
This will return all events where a quote with ask size bigger than 1000 is followed within ten seconds by a quote with ask size smaller than ten, which is in turn followed within 20 seconds by a quote with as size bigger than 1000 again, for each ticker. The `anchors` define the observations, and can be any custom predicate or SQL expression.

The result is another OrderedStream, with one row for each event detected, and one column for each anchor denoting the timestamp of the corresponding observations that made up the event. You can also return custom expressions for each observation instead of the timestamp, similar to the `measures` clause in SQL's `MATCH_RECOGNIZE`.

I don't mean to brag, but I think this feature is pretty cool.

## Bye

This is all for now. If you are interested in using Quokka, please join the [Discord](https://discord.gg/6ujVV9HAg3), shoot me an email (zihengw@stanford.edu) or raise a Github issue!
