#Tutorials

This section is for learning how to use Quokka's DataStream API. **Quokka's DataStream API is basically a dataframe API.** It takes heavy inspiration from SparkSQL and Polars, and adopts a lazy execution model. This means that in contrast to Pandas, your operations are not executed immediately after you define them. Instead, Quokka builds a logical plan under the hood and executes it only when the user wants to "collect" the result, just like Spark. 

For the first part of our tutorial, we are going to go through implementing a few SQL queries in the TPC-H benchmark suite. You can download the data [here](https://drive.google.com/file/d/14yDfWZUAxifM5i7kf7CFvOQCIrU7sRXP/view?usp=sharing). It is about 1GB unzipped. Please download the data (should take 2 minutes) and extract it to some directory locally. If you are testing this on a VM where clicking the link can't work, try this command after pip installing gdown: `gdown https://drive.google.com/uc?id=14yDfWZUAxifM5i7kf7CFvOQCIrU7sRXP`. If it complain gdown not found, maybe you need to do something like this: `/home/ubuntu/.local/bin/gdown`. The SQL queries themselves can be found on this awesome [interface](https://umbra-db.com/interface/).

These tutorials will use your local machine. They shouldn't take too long to run. It would be great if you can follow along, not just for fun -- **if you find a bug in this tutorial I will buy you a cup of coffee!**

You can also refer to the API reference on the index.

## Lesson -1: Things

Please read the [Getting Started](started.md) section. I spent way too much time making the cartoons on that page.

## Lesson 0: Reading Things

For every Quokka program, we need to set up a `QuokkaContext` object. This is similar to the Spark `SQLContext`. This can easily be done by running the following two lines of code in your Python terminal.

~~~python
from pyquokka.df import * 
qc = QuokkaContext()
~~~

Once we have the `QuokkaContext` object, we can start reading data to obtain DataStreams. Quokka can read data on disk and on the cloud (currently S3). For the purposes of this tutorial we will be reading data from disk. Quokka currently reads CSV and Parquet, with plans to add JSON soon. 

Here is how you would read a CSV file **if you know the schema**:

~~~python
# the last column is called NULL, because the TPC-H data generator likes to put a | at the end of each row, making it appear as if there is a final column
# with no values. Don't worry, we can drop this column. 
lineitem_scheme = ["l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice", "l_discount","l_tax","l_returnflag","l_linestatus","l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct","l_shipmode","l_comment", "null"]
lineitem = qc.read_csv(disk_path + "lineitem.tbl", lineitem_scheme, sep="|")
~~~

And if you don't know the schema but there is a header row where column names are **separated with the same separator as the data**:

~~~python
lineitem = qc.read_csv(disk_path + "lineitem.tbl", sep="|", has_header=True)
~~~

The test files you just downloaded are of this form. No need to specify the schema for those. In this case Quokka will just use the header row for the schema.

You can also read a directory of CSV files:

~~~python
lineitem = qc.read_csv(disk_path + "lineitem/*", lineitem_scheme, sep="|", has_header = True)
~~~

Now let's read all the tables of the TPC-H benchmark suite. Set `disk_path` to where you unzipped the files.
~~~python
lineitem = qc.read_csv(disk_path + "lineitem.tbl", sep="|", has_header=True)
orders = qc.read_csv(disk_path + "orders.tbl", sep="|", has_header=True)
customer = qc.read_csv(disk_path + "customer.tbl",sep = "|", has_header=True)
part = qc.read_csv(disk_path + "part.tbl", sep = "|", has_header=True)
supplier = qc.read_csv(disk_path + "supplier.tbl", sep = "|", has_header=True)
partsupp = qc.read_csv(disk_path + "partsupp.tbl", sep = "|", has_header=True)
nation = qc.read_csv(disk_path + "nation.tbl", sep = "|", has_header=True)
region = qc.read_csv(disk_path + "region.tbl", sep = "|", has_header=True)
~~~

If you want to read the Parquet files, you should first run this script to generate the Parquet files:
~~~python
import polars as pl
disk_path = "/home/ubuntu/tpc-h/" #replace
files = ["lineitem.tbl","orders.tbl","customer.tbl","part.tbl","supplier.tbl","partsupp.tbl","nation.tbl","region.tbl"]
for file in files:
    df = pl.read_csv(disk_path + file,sep="|",has_header = True, parse_dates = True).drop("null")
    df.write_parquet(disk_path + file.replace("tbl", "parquet"), row_group_size=100000)
~~~

To read in a Parquet file, you don't have to worry about headers or schema, just do:
~~~python
lineitem = qc.read_parquet(disk_path + "lineitem.parquet")
~~~

Currently, `qc.read_csv` and `qc.read_parquet` will either return a DataStream or just a Polars DataFrame directly if the data size is small (set at 10 MB).

## Lesson 1: Doing Things

Now that we have read the data, let's do things with it. First, why don't we count how many rows there are in the `lineitem` table.

~~~python
lineitem.count()
~~~

If you don't see the number 6001215 after a while, something is very wrong. Please send me an email, I will help you fix things (and buy you a coffee): zihengw@stanford.edu.

Feel free to type other random things and see if it's supported, but for those interested, let's follow a structured curriculum. Let's take a look at [TPC-H query 1](https://github.com/dragansah/tpch-dbgen/blob/master/tpch-queries/1.sql).

This is how you would write it in Quokka. This is very similar to how you'd write in another DataFrame library like Polars or Dask.

~~~python
def do_1():
    d = lineitem.filter_sql("l_shipdate <= date '1998-12-01' - interval '90' day")
    d = d.with_columns({"disc_price": d["l_extendedprice"] * (1 - d["l_discount"]), 
                        "charge": d["l_extendedprice"] * (1 - d["l_discount"]) * (1 + d["l_tax"])})
    
    f = d.groupby(["l_returnflag", "l_linestatus"], orderby=["l_returnflag","l_linestatus"]).agg({"l_quantity":["sum","avg"], "l_extendedprice":["sum","avg"], "disc_price":"sum", 
        "charge":"sum", "l_discount":"avg","*":"count"})
    return f.collect()
~~~

Quokka supports filtering DataStreams by `DataStream.filter()`. Filters can be specified in SQL syntax. The columns in the SQL expression must exist in the schema of the DataStream. A more Pythonic way of doing this like `b = b[b.a < 5]` isn't supported yet, mainly due to the finickiness surrounding date types etc. The result of a `filter()` is another DataStream whose Polars DataFrames will only contain rows that respect the predicate.

On the plus side, Quokka uses the amazing [SQLGlot](https://github.com/tobymao/sqlglot) library to support most ANSI-SQL compliant predicates, including dates, between, IN, even arithmetic in conditions. Try out some different [predicates](datastream/filter.md)! Please give SQLGlot a star when you're at it. For example, you can specify this super complicated predicate for [TPC-H query 6](https://github.com/dragansah/tpch-dbgen/blob/master/tpch-queries/6.sql):

~~~python
def do_6():
    d = lineitem.filter("l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 24")
    d = d.with_columns({"revenue": lambda x: x["l_extendedprice"] * x["l_discount"]}, required_columns={"l_extendedprice", "l_discount"})
    f = d.aggregate({"revenue":["sum"]})
    return f.collect()
~~~

Quokka supports creating new columns in DataStreams with `with_columns`. Read more about how this works [here](datastream/with_columns.md). This is in principle similar to Spark `df.with_column` and Pandas UDFs. You can also use `with_columns_sql`, documented [here](datastream/with_columns_sql.md).

Like most Quokka operations, `with_columns` will produce a new DataStream with an added column and is not inplace. This means that the command is lazy, and won't trigger the runtime to produce the actual data. It simply builds a logical plan of what to do in the background, which can be optimized when the user specifically ask for the result.

Finally, we can group the DataStream and aggregate it to get the result. Read more about aggregation syntax [here](datastream/grouped_agg.md). The aggregation will produce another DataStream, which we call `collect()` on, to convert it to a Polars DataFrame in your Python terminal.

When you call `.collect()`, the logical plan you have built is actually optimized and executed. This is exactly how Spark works. To view the optimized logical plan and learn more about what Quokka is doing, you can do `f.explain()` which will produce a graph, or `f.explain(mode="text")` which will produce a textual explanation.

Joins work very intuitively. For example, this is how to do [TPC-H query 12](https://github.com/dragansah/tpch-dbgen/blob/master/tpch-queries/12.sql).
~~~python
def do_12():
    d = lineitem.join(orders,left_on="l_orderkey", right_on="o_orderkey")
    d = d.filter("l_shipmode IN ('MAIL','SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and \
        l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1995-01-01'")
    f = d.groupby("l_shipmode").agg_sql("""
        sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,
        sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
    """)
    return f.collect()
~~~

Note it does not matter if you filter after the join or before the join, Quokka will automatically push them down during the logical plan optimization. The `join` operator on a DataStream takes in either another DataStream or a Polars DataFrame in your Python session. In the latter case, this Polars DataFrame will be broadcasted to different workers similar to Spark's broadcast join. Here is another example, [TPC-H query 3](https://github.com/dragansah/tpch-dbgen/blob/master/tpch-queries/3.sql).

~~~python
def do_3():
    d = lineitem.join(orders,left_on="l_orderkey", right_on="o_orderkey")
    d = customer.join(d,left_on="c_custkey", right_on="o_custkey")
    d = d.filter("c_mktsegment = 'BUILDING' and o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15'")
    d = d.with_columns({"revenue": d["l_extendedprice"] * ( 1 - d["l_discount"])})
    f = d.groupby(["l_orderkey","o_orderdate","o_shippriority"]).agg({"revenue":["sum"]})
    return f.collect()
~~~

Note unlike some SQL engines, Quokka currently will not try to figure out the optimal join ordering between the specified three-way join between lineitem, orders and customer tables. You are responsible for figuring that out at the moment -- try to join smaller tables first and then join them against larger tables, or try to minimize the intermeidate result size from those joins.

An important thing to note is that Quokka currently only support inner joins. Other kinds of joins are coming soon.

Feel free to look at some other queries in the Quokka [github](https://github.com/marsupialtail/quokka/tree/master/apps), or browse the API references. While you are there, please give Quokka a star!

##Lesson 2: Writing Things
So far, we have just learned about how to read things into DataStreams and do things to DataStreams. You can also write out DataStreams to persistent storage like disk or S3 to record all the amazing things we did with them.

Quokka currently operates like Spark and by default writes a directory of files, with a default maximum file size for different file formats. This makes it easy to perform parallel writing.

To write out a DataStream to CSV or Parquet to a local directory (you must specify a valid absolute path), simply do:

~~~python
d.write_csv("/home/ubuntu/test-path/")
d.write_parquet("/home/ubuntu/test-path/")
~~~

To write out a DataStream to S3, you should specify an S3 bucket and prefix like this:

~~~python
d.write_csv("s3://bucket/prefix/")
d.write_parquet("s3://bucket/prefix/")
~~~

Writing out a DataStream is a blocking API and will automatically call a `collect()` for you. The collected Polars DataFrame at the end is just a column of filenames produced.

##Lesson 3: Things you can't do.

Here is a brief discussion of what Quokka is not great for. Quokka's main advantage stems from the fact it can pipeline the execution of DataStreams. Once a partition (typically a Polars DataFrame) in a DataStream has been generated, it can be immediately consumed by a downstream user. This means downstream processing of this partition and upstream generation of the next partition can be overlapped. 

Now, if an operator processing a DataStream cannot emit any partitions downstream until it has seen all of the partitions in its input DataStreams, the pipeline breaks. An example of this is an aggregation. You cannot safely emit the result of a sum of a column of a table until you have seen every row! The main examples of this in data processing are groupby-aggregations and distributed sorts. 

Currently, calling `groupby().agg()` or just `agg()` on a DataStream will produce another DataStream. However that DataStream will consist of exactly one batch, which holds the final result, emitted when it's computed. It is recommended to just call `collect()` or `compute()` on that result. 

Quokka currently does not support distributed sort -- indeed a sort heavy workload is really great for Spark. Distributed sorting is not exactly needed for many analytical SQL workloads since you typically do the aggregation before the order by, which greatly reduce the number of rows you have to sort. You can then sort after you have done `collect()`. However for many other workloads distributed sorting is critical, and Quokka aims to support this as soon as possible.

Things that Quokka can do and doesn't do yet: fine grained placement of UDFs or UDAFs on GPUs or CPUs, core-count-control, Docker support, reading JSON, etc. Most of these can be easily implemented (and some already are) in the graph level API, however it takes effort to figure out what's the best abstractions to expose in the DataStream API. If you want to make this list shorter, I welcome contributions: zihengw@stanford.edu.
