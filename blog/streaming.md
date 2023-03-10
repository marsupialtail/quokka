# Backfilling real-time features is a probabilistic affair
Let's assume we are trying to use Apache Flink to compute real-time features for machine learning. Typically the setup looks like this. We got Kafka topics streaming in different event streams. We might join these streams and apply a bunch of windows and such in Flink to compute our features. We use a Flink sink to write our features back out to another Kafka topic or directly into Redis, let's just say Redis.

If we think about how we are going to backfill a new feature in this setup, what are we going to do?

First, what is backfilling a feature? Backfilling a feature means I would like to know if I had ran this new feature definition in the past, **what** feature values would be written to Redis and **when** they would be written. You probably desire the same if features are written out to a Kafka topic -- you want to know what is the timestamp of the feature and its value. 

*Why is the timestamp important in the backfill?* 

You are probably going to test the usefulness of your feature on historical prediction events, which happened at discrete points in time. You probably would like to know what's the latest value of that feature **at the time the prediction event occurred**, along with the "asof" values for all the other features.

Okay now we know what we want, let's think about what input data we have. Typically the input event streams are Kafka topics, and they are stored in Kafka or on cheap object storage like S3. Either way, the records are usually stored partitioned by a partition key and ordered within the partition in the order in which they arrived at the Kafka cluster. The records usually come with a timestamp, which is typically *the system time on the machine that produced the Kafka record*, not the time the record arrived at the Kafka cluster. From the perspective of Flink, this can also be called *event time*, i.e. when the event actually occurred IRL. This means the records are roughly ordered by this timestamp, though (severe) out-of-order events could occur. These give rise to the use of watermarks, late event handling, dead letter queue, etc. etc. 

Note we typically *cannot* record the processing time of each record, i.e. when it's processed by Flink. Even if we can it doesn't matter because most people's historical data don't contain this information today.

I am going to assert right now that *for most Flink jobs, accurate backfill is impossible given the available historical data*. For some Flink jobs, we can get pretty close, for others we have to make some serious assumptions on the relationship between event time and processing time to make any kind of progress. 

Let's take a simple Flink job, where we key the input Kafka stream by a partition key and then do a rolling window average within each key with five minute windows. Remember our questions:  **what** feature values would be written to Redis and **when** they would be written? In this case, these questions basically translate to: **what** events are included in each window and **when** are the windows emitted?

Let's think about the second question first: **when** are the windows emitted? Flink supports three kinds of window computations. 
- **Processing time windows**: windows are emitted periodically based on system time of the processor running the window operator.
- **Event time windows**: windows are emitted based on watermarks based on the event time. 
- **Per-record firing**: partial window results for the current window are emitted every time a new record comes in. This strategy still needs a processing-time or event-time criteria for closing a window.

Let's think about these one by one. First, **processing time windows**. Well **when** the windows are emitted in this case is a very easy question to answer, if we assume synchronized clocks (we shall assume that, clock skew will be the least of our worries). The harder question is assuming processing time windows, **what** events are included in each window. 

For processing time windows, this answer can *not* be answered exactly with the historical data we have. We know the order in which records are processed in each Kafka partition, but we don't know when those records arrived in Flink. For that matter -- consider the case where an entire partition is delayed, e.g. if the Kafka machine hosting that partition is kinda slow. Then records in that entire input partition might not show up in processing time windows for a bit!

To give any answer to this question, we have to make assumptions on the processing times of the records. For example, we can assume that records are typically processed within one minute after they are produced. Note the world *typically*, since late events obviously will not easily follow this assumption. What assumptions then? And how do we answer this question after the assumptions? Let's hold our thoughts and turn to the other two kinds of windows.

For **event time windows**, the question of **what** events are included in a window is relatively easier to answer but the question of **when** the windows are emitted is now much harder to answer. This is because an event-time window is emitted *when the watermark passes the end of window*. Thus we have to somehow stipulate when in *historical processing time* the watermark passed some *event time* boundary. 

This quickly leads us to demand similar kinds of assumptions as the ones we need to answer questions about processing time windows, i.e. there must be some kind of bounded difference between event time and processing time of records.

What about the third kind of firing strategy, where we update results every record? First we should probably think what we want out of this kind of strategy, and what is the definition of correctness here. Why do we want to update results every record? This is actually a quite common use case when you want to make decisions based on features you compute for *this window*, e.g. session window. (Unfortunately FlinkSQL session window doesn't support this firing strategy) Frameworks like AirSkiff use this triggering strategy. 

In this kind of firing strategy, the exact order in which the records are processed matters if we want to get all the feature outputs. However, we are usually only interested in the latest feature value asof a certain processing time. Then we just need to figure out **at that processing time, what records are in the current window**, which is not too different from what we need to do with processing time windows. Of course this only works for window computations where the order of elements inside a window don't matter for the evaluation, like sums and count. If the order matters then the exact order is needed.

## Digression: what is streaming good for?

Now that we know we cannot backfill things exactly and have to make assumptions to even make approximations, we should discuss what things like Flink is actually used for in practice.

Most people use Flink to do things with latency requirements between a few seconds to a few minutes. Anything above that you should probably just use Spark or some database and anything below that you should roll your own stream processing system on bare metal.

This is important because this gives us a guide as to how much we can mess up processing time estimates by. In a workload with 200 millsecond latency where new results pop out every 200 ms, if your timestamp estimates are typically off by more than 200 ms, then your backfill is pretty much going to be useless. 

Similarly in a workload where the target latency is a few hours and millions of events are processed for each update, who cares about handling a few late events here and there properly or when exactly in a 1 second range the feature actually gets computed?

## Rules of engagement: processing time assumptions

Okay. Now let's proceed with trying to estimate processing timestamps for our input Kafka records, which only have event time (i.e. the timestamp on the producer machine). We can come up with two uncontroversial rules that must hold:
- A record Y that comes after a record X in the Kafka partition must have processing time larger than the processing time of X plus a fixed delay D1 (which can be 0).
- A record X's processing time must be larger than its event time by a fixed delay D2 (which can be 0).

For example, if we the records in the Kafka partition have event time 10, 7, 11, 12, 15, 9, then a valid processing time sequence could be 10, 10, 11, 12, 15, 15. Another valid processing time sequence could be 100, 100, 100, 100, 100, 100. The first seems more reasonable than the second, so we add in the additional assumption that a record's processing time is the **minimum value** that satisfies our two constraints. Then the first sequence becomes the *only* admissible processing time sequence. If D1 and D2 are zero then this is the cumulative maximum of the event times.

Note the processing time sequence is filled in for each Kafka partition separately since the order of records, needed for the first rule, only makes sense within a single Kafka partition. Concretely, let's say we have a table of Kafka records with schema *(partition_id, partition_offset, event_time, value)*, then to fill in the processing time for each record we would do the following SQL query:

```
 select *, max(event_time) over win as processing_time
 from kafka_records
 window win as (
	 partition by partition_id,
	 order by partition_offset asc,
	 rows between unbounded preceding and current row)
```

Given this filled in processing time for these records, i.e. when they were projected to be first ingested into Flink, we can now say what the watermark generator would do. In Flink the watermark generator runs at the source and injects watermark messages in the source stream either every record or periodically. Since these are actual data messages, injecting every record usually incurs very high performance overhead so it's usually done periodically. 

For the Kafka source reader, the input partitions are split up between different readers. Each reader's watermark is the minimum of the maximum event times it has seen among its assigned partitions. In Flink, downstream nodes emit as watermark the minimum of all upstream watermarks. In our model where all the source readers have synchronized clocks and are generating watermarks periodically with the same interval, we can then say the watermark arriving at a downstream operator from the source at a certain processing time can be given by the maximum event time across all the Kafka partitions seen at that time.

Now that we are talking about downstream operators, we should think about how they are going to treat watermarks. Here is what Flink says about one such operator: "`WindowOperator` will first evaluate all windows that should be fired, and only after producing all of the output triggered by the watermark will the watermark itself be sent downstream. In other words, all elements produced due to occurrence of a watermark will be emitted before the watermark."

So if the WindowOperator receives watermark 10 at processing time 12, and if watermark 10 happens to mark the completion of an event time window it's computing, it will first output the results for this window and then pass on this watermark. When in processing time does these two things happen? **We have no way of knowing without knowing how long it took for the operator to do the computation**. More assumptions! We can assume a fixed delay here, or we can just assume 0, if we assume that the time gap between successive input records is a lot larger than the processing time of Flink operators.

Now we can answer our questions we posed a long time ago: **when** processing time, event time, and per-record windows emit results, and **what** records are included in each window. We can even use SQL on historical batch data to answer those questions, to arrive at a target table with schema *(processing_time, feature_value)*. This is what we want! 

## External data sources

There are two kinds of data organizations typically have. Events and state. Events record things that happened, state record state of the world that may change, which could be computed from events. Let's give some examples. We assume state is SCD type II, and we add records so we can get the state at a point in time in the past. This can be done with time travel in a data lake, database change-data-capture logs etc.

Some examples:
- Finance: trades, quotes, earnings announcements, news, etc. are events. Order book, tradeable stocks, stock market cap etc. are state. 
- Security: internet packets, monitoring traces are events. Routing information is state.
- Retail & E-commerce: purchases, views, clicks are events. Customer and product information is state. 

Why is this important? A lot of streaming workflows are reasonably expected to consult external information, especially the so-called "state" variables. For example, if you have customer information stored in DynamoDB, then your real-time feature engineering job in Flink could very well poll that information to compute the feature.

How would backfills work in this case? Needless to say, **historical data must be available for the state**. If your feature relies on customer information, you must have a way to know what that information *would have been at a point in time in the past*. 

Given a Kafka record, e.g. a view event on an ad, *which point in time* should we poll the associated customer information? Well we have already decided how we are going to guess this Kafka record's processing time, and based on our assumptions on operator latencies when the record will be processed. This tells us which historical snapshot we need of the state data.

How to get this historical state data? It really depends on how your data is stored. A few scenarios:
- Your state data is stored as type II SCD with new rows corresponding to updates. Then fetching the state for each Kafka record amounts to an asof join on the time columns after you have filled in the guessed processing times for the Kafka records.
- Your state data is stored in DynamoDB with CDC and historical snapshots. Then fetching the state for a Kafka record amounts to replaying the CDC log to figure out what the state is at that point in time. This is an asof join on the CDC log with lookups to the closest snapshot for things that are not found in the CDC log (i.e. not changed from the snapshot).
- Your state data is stored in a data lake format with time travel. You are now effectively doing an asof join against different snapshots. Good luck since no existing SQL engine supports this, though this can theoretically be done.
- Your state data doesn't change. Thank the Lord.
