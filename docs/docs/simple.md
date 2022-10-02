#Tutorials

This section is for learning how to use Quokka's DataStream API. **Quokka's DataStream API is basically a dataframe API.** It takes heavy inspiration from SparkSQL and Polars, and adopts a lazy execution model. This means that in contrast to Pandas, your operations are not executed immediately after you define them. Instead, Quokka builds a logical plan under the hood and executes it only when the user wants to "collect" the result, just like Spark. 

For the first part of our tutorial, we are going to go through implementing a few SQL queries in the TPC-H benchmark suite. You can download the data [here](https://drive.google.com/file/d/1a4yhPoknXgMhznJ9OQO3BwHz2RMeZr8e/view?usp=sharing). It is about 1GB unzipped. Please download the data (should take 2 minutes) and extract it to some directory locally. The SQL queries themselves can be found on this awesome [interface](https://umbra-db.com/interface/).

These tutorials will use your local machine. They shouldn't take too long to run. It would be great if you can follow along, not just for fun -- **if you find a bug in this tutorial I will buy you a cup of coffee!**

For an extensive API reference, please refer to [here](datastream.md).

## Lesson -1: Things

Please read the [Getting Started](started.md) section 

## Lesson 0: Reading Things

For every Quokka program, we need to set up a `QuokkaContext` object. This is similar to the Spark `SQLContext`. This can easily be done by running the following two lines of code in your Python terminal.

~~~python
from pyquokka.df import * 
qc = QuokkaContext()
~~~

Once we have the `QuokkaContext` object, we can start reading data to obtain DataStreams. Quokka can read data on disk and on the cloud (currently S3). For the purposes of this tutorial we will be reading data from disk. Quokka currently reads CSV and Parquet, with plans to add JSON soon. 

Here is how you would read a CSV file if you know the schema:

~~~python
# the last column is called NULL, because the TPC-H data generator likes to put a | at the end of each row, making it appear as if there is a final column
# with no values. Don't worry, we can drop this column. 
lineitem_scheme = ["l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice", "l_discount","l_tax","l_returnflag","l_linestatus","l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct","l_shipmode","l_comment", "null"]
lineitem = qc.read_csv(disk_path + "lineitem.tbl", lineitem_scheme, sep="|")
~~~

And if you don't know the schema but there is a header row where column names are **separated with the same separator as the data**:

~~~python
lineitem = qc.read_csv(disk_path + "lineitem.tbl.named", sep="|", has_header=True)
~~~

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

## Lesson 1: Doing Things

Now that we have read the data, let's do things with it. First, why don't we count how many rows there are in the `lineitem` table.

~~~python
>>> lineitem.aggregate({"*":"count"}).collect()
~~~

If you don't see the number 6001215 after a while, something is very wrong. Please send me an email, I will help you fix things (and buy you a coffee): zihengw@stanford.edu.

Feel free to type other random things and see if it's supported, but for those interested, let's follow a structured curriculum. Let's take a look at [TPC-H query 1](https://github.com/dragansah/tpch-dbgen/blob/master/tpch-queries/1.sql).

This is how you would write it in Quokka. This is very similar to how you'd write in another DataFrame library like Polars or Dask.

~~~python
def do_1():

    d = lineitem.filter("l_shipdate <= date '1998-12-01' - interval '90' day")
    d = d.with_column("disc_price", lambda x: x["l_extendedprice"] * (1 - x["l_discount"]), required_columns ={"l_extendedprice", "l_discount"})
    d = d.with_column("charge", lambda x: x["l_extendedprice"] * (1 - x["l_discount"]) * (1 + x["l_tax"]), required_columns={"l_extendedprice", "l_discount", "l_tax"})

    f = d.groupby(["l_returnflag", "l_linestatus"], orderby=["l_returnflag","l_linestatus"]).agg({"l_quantity":["sum","avg"], "l_extendedprice":["sum","avg"], "disc_price":"sum", "charge":"sum", "l_discount":"avg","*":"count"})
        
    return f.collect()
~~~

Quokka supports filtering DataStreams by `DataStream.filter()`. Filters can be specified in SQL syntax. The columns in the SQL expression must exist in the schema of the DataStream. A more Pythonic way of doing this like `b = b[b.a < 5]` isn't supported yet, mainly due to the finickiness surrounding date types etc.

On the plus side, Quokka uses the amazing [SQLGlot](https://github.com/tobymao/sqlglot) library to support most ANSI-SQL compliant predicates, including dates, between, IN, even arithmetic in conditions. Try out some different [predicates](datastream.md#filter)! Please give SQLGlot a star when you're at it. For example, you can specify this super complicated predicate for TPC-H query 6:

~~~python
def do_6():
    d = lineitem.filter("l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 24")
    d = d.with_column("revenue", lambda x: x["l_extendedprice"] * x["l_discount"], required_columns={"l_extendedprice", "l_discount"})
    f = d.aggregate({"revenue":["sum"]})
    return f.collect()
~~~

Quokka supports creating new columns in DataStreams with `with_column`. Read more about how this works [here](datastream.md#with_column). This is in principle similar to Spark `df.with_column` and Polars [`with_column`](https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.DataFrame.with_columns.html). 

Here is another example of 

~~~python
def do_12():
    
    d = lineitem.join(orders,left_on="l_orderkey", right_on="o_orderkey")
    
    d = d.filter("l_shipmode IN ('MAIL','SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and \
        l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1995-01-01'")

    d = d.with_column("high", lambda x: (x["o_orderpriority"] == "1-URGENT") | (x["o_orderpriority"] == "2-HIGH"), required_columns={"o_orderpriority"})
    d = d.with_column("low", lambda x: (x["o_orderpriority"] != "1-URGENT") & (x["o_orderpriority"] != "2-HIGH"), required_columns={"o_orderpriority"})

    f = d.groupby("l_shipmode").aggregate(aggregations={'high':['sum'], 'low':['sum']})
    return f.collect()
~~~

So far we have just played with the lineitem table. Let's do some joins!

~~~python
def do_3():
    d = lineitem.join(orders,left_on="l_orderkey", right_on="o_orderkey")
    d = customer.join(d,left_on="c_custkey", right_on="o_custkey")
    d = d.filter("c_mktsegment = 'BUILDING' and o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15'")
    d = d.with_column("revenue", lambda x: x["l_extendedprice"] * ( 1 - x["l_discount"]) , required_columns={"l_extendedprice", "l_discount"})

    f = d.groupby(["l_orderkey","o_orderdate","o_shippriority"]).agg({"revenue":["sum"]})
    return f.collect()
~~~

And here is a much more complicated join query, TPC-H query 5.

~~~python
def do_5():

    '''
    Quokka currently does not pick the best join order, or the best join strategy. This is upcoming improvement for a future release.
    You will have to pick the best join order. One way to do this is to do sparksql.explain and "borrow" Spark Catalyst CBO's plan.
    As a general rule of thumb you want to join small tables first and then bigger ones.
    '''

    asia = region.filter(region["r_name"] == "ASIA")
    asian_nations = nation.join(asia, left_on="n_regionkey",right_on="r_regionkey").select(["n_name","n_nationkey"])
    d = customer.join(asian_nations, left_on="c_nationkey", right_on="n_nationkey")
    d = d.join(orders, left_on="c_custkey", right_on="o_custkey", suffix="_3")
    d = d.join(lineitem, left_on="o_orderkey", right_on="l_orderkey", suffix="_4")
    d = d.join(supplier, left_on="l_suppkey", right_on="s_suppkey", suffix="_5")
    d = d.filter("s_nationkey = c_nationkey and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year")
    d = d.with_column("revenue", lambda x: x["l_extendedprice"] * ( 1 - x["l_discount"]) , required_columns={"l_extendedprice", "l_discount"})
    f = d.groupby("n_name").agg({"revenue":["sum"]})

    return f.collect()
~~~





~~~python
def do_7():
    d1 = customer.join(nation, left_on = "c_nationkey", right_on = "n_nationkey")
    d1 = d1.join(orders, left_on = "c_custkey", right_on = "o_custkey", suffix = "_3")
    d2 = supplier.join(nation, left_on="s_nationkey", right_on = "n_nationkey")
    d2 = lineitem.join(d2, left_on = "l_suppkey", right_on = "s_suppkey", suffix = "_3")
    
    d = d1.join(d2, left_on = "o_orderkey", right_on = "l_orderkey",suffix="_4")
    d = d.rename({"n_name_4": "supp_nation", "n_name": "cust_nation"})
    d = d.filter("""(
                                (supp_nation = 'FRANCE' and cust_nation = 'GERMANY')
                                or (supp_nation = 'GERMANY' and cust_nation = 'FRANCE')
                        )
                        and l_shipdate between date '1995-01-01' and date '1996-12-31'""")
    d = d.with_column("l_year", lambda x: x["l_shipdate"].dt.year(), required_columns = {"l_shipdate"})
    d = d.with_column("volume", lambda x: x["l_extendedprice"] * ( 1 - x["l_discount"]) , required_columns={"l_extendedprice", "l_discount"})
    f = d.groupby(["supp_nation","cust_nation","l_year"], orderby=["supp_nation","cust_nation","l_year"]).aggregate({"volume":"sum"})
    return f.collect()
~~~


