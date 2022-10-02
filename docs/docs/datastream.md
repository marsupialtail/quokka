#


## DataStream
[source](https://github.com/blob/master/datastream.py/#L12)
```python 
DataStream(
   quokka_context, schema: list, source_node_id: int
)
```


---
Quokka DataStream class is how most users are expected to interact with Quokka.
However users are not expected to create a DataStream directly by calling its constructor.
Note that constructor takes an argument called `source_node_id`, which would confuse 
most data scientists -- even me!


**Args**

* **quokka_context** (pyquokka.df.QuokkaContext) : Similar to Spark SQLContext.
* **schema** (list) : The schema of this DataStream, i.e. a list of column names. We might change it to be 
    a dictionary with type information in the future to do better static code checking.
* **source_node_id** (int) : the node in the logical plan that produces this DataStream.


**Attributes**

* **quokka_context** (pyquokka.df.QuokkaContext) : Similar to Spark SQLContext.
* **schema** (list) : The schema of this DataStream, i.e. a list of column names. We might change it to be 
    a dictionary with type information in the future to do better static code checking.
* **source_node_id** (int) : the node in the logical plan that produces this DataStream.



**Methods:**


### .collect
[source](https://github.com/blob/master/datastream.py/#L45)
```python
.collect()
```

---
This will trigger the execution of computational graph, similar to Spark collect(). 
The result will be a Polars DataFrame on the master

Return:
Polars DataFrame. 


**Examples**

~~~python
>>> f = qc.read_csv("my_csv.csv")

>>> result = f.collect() # result will be a Polars dataframe, as if you did polars.read_csv("my_csv.csv")
~~~

### .compute
[source](https://github.com/blob/master/datastream.py/#L63)
```python
.compute()
```

---
This will trigger the execution of computational graph, similar to Spark collect
The result will be a Quokka DataSet, which you can then call to_df() or call to_stream() to initiate another computation.

Return:
Quokka Quokka DataSet. Currently this is going to be just a list of objects distributed across the Redis servers on the workers.

### .explain
[source](https://github.com/blob/master/datastream.py/#L74)
```python
.explain(
   mode = 'graph'
)
```

---
This will not trigger the execution of your computation graph but will produce a graph of the execution plan. 

**Args**

* **mode** (str) : 'graph' will show a graph, 'text' will print a textual description.

---
Return:
    None.

### .write_csv
[source](https://github.com/blob/master/datastream.py/#L85)
```python
.write_csv(
   table_location, output_line_limit = 1000000
)
```

---
This will write out the entire contents of the DataStream to a list of CSVs. This is a blocking operation, and will
call `collect()` under the hood.


**Args**

* **table_lcation** (str) : the root directory to write the output CSVs to. Similar to Spark, Quokka by default
    writes out a directory of CSVs instead of dumping all the results to a single CSV so the output can be
    done in parallel. If your dataset is small and you want a single file, you can adjust the output_line_limit
    parameter. Example table_locations: s3://bucket/prefix for cloud, absolute path /home/user/files for disk.
* **output_line_limit** (int) : how many rows each CSV in the output should have. The current implementation simply buffers
    this many rows in memory instead of using file appends, so you should have enough memory!

---
Return:
    Polars DataFrame containing the filenames of the CSVs that were produced. 


**Examples**

~~~python
>>> f = qc.read_csv("lineitem.csv")

>>> f = f.filter("l_orderkey < 10 and l_partkey > 5")

>>> f.write_csv("/home/user/test-out") # you should create the directory before hand.
~~~

### .write_parquet
[source](https://github.com/blob/master/datastream.py/#L152)
```python
.write_parquet(
   table_location, output_line_limit = 10000000
)
```

---
This will write out the entire contents of the DataStream to a list of Parquets. This is a blocking operation, and will
call `collect()` under the hood. By default, each output Parquet file will contain one row group.


**Args**

* **table_lcation** (str) : the root directory to write the output Parquets to. Similar to Spark, Quokka by default
    writes out a directory of Parquets instead of dumping all the results to a single Parquet so the output can be
    done in parallel. If your dataset is small and you want a single file, you can adjust the output_line_limit
    parameter. Example table_locations: s3://bucket/prefix for cloud, absolute path /home/user/files for disk.
* **output_line_limit** (int) : the row group size in each output file.

---
Return:
    Polars DataFrame containing the filenames of the Parquets that were produced. 


**Examples**

~~~python
>>> f = qc.read_csv("lineitem.csv")

>>> f = f.filter("l_orderkey < 10 and l_partkey > 5")

>>> f.write_parquet("/home/user/test-out") # you should create the directory before hand.
~~~

### .filter
[source](https://github.com/blob/master/datastream.py/#L215)
```python
.filter(
   predicate: str
)
```

---
This will filter the DataStream to contain only rows that match a certain predicate. Currently this predicate must be specified
in SQL syntax. You can write any SQL clause you would generally put in a WHERE statement containing arbitrary conjunctions and 
disjunctions. The identifiers however, must be in the schema of this DataStream! We aim to soon support a more Pythonic interface
that better resembles Pandas which allows you to do things like d = d[d.a > 10]. Please look at the examples below. 

Since a DataStream is implemented as a stream of batches, you might be tempted to think of a filtered DataStream as a stream of batches where each
batch directly results from a filter being applied to a batch in the source DataStream. While this certainly may be the case, filters
are aggressively optimized by Quokka and is most likely pushed all the way down to the input readers. As a result, you typically should
not see a filter node in a Quokka execution plan shown by `explain()`. 

It is much better to think of a DataStream simply as a stream of rows that meet certain criteria, and who may be non-deterministically 
batched together by the Quokka runtime. Indeed, Quokka makes no guarantees on the sizes of these batches, which is determined at runtime. 
This flexibility is an important reason for Quokka's superior performance.


**Args**

* **predicate** (str) : a SQL WHERE clause, look at the examples.

---
Return:
    A DataStream consisting of rows from the source DataStream that match the predicate.


**Examples**

~~~python
>>> f = qc.read_csv("lineitem.csv")

# filter for all the rows where l_orderkey smaller than 10 and l_partkey greater than 5
>>> f = f.filter("l_orderkey < 10 and l_partkey > 5") 

# nested conditions are supported
>>> f = f.filter("l_orderkey < 10 and (l_partkey > 5 or l_partkey < 1)") 

# most SQL features such as IN and date are supported.
>>> f = f.filter("l_shipmode IN ('MAIL','SHIP') and l_receiptdate < date '1995-01-01'")

# you can do arithmetic in the predicate just like in SQL. 
>>> f = f.filter("l_shipdate < date '1994-01-01' + interval '1' year and l_discount between 0.06 - 0.01 and 0.06 + 0.01")

# this will fail! Assuming c_custkey is not in f.schema
>>> f = f.filter("c_custkey > 10")
~~~

### .select
[source](https://github.com/blob/master/datastream.py/#L270)
```python
.select(
   columns: list
)
```

---
This will create a new DataStream that contains only selected columns from the source DataStream.

Since a DataStream is implemented as a stream of batches, you might be tempted to think of a filtered DataStream as a stream of batches where each
batch directly results from selecting columns from a batch in the source DataStream. While this certainly may be the case, `select()` is aggressively 
optimized by Quokka and is most likely pushed all the way down to the input readers. As a result, you typically should
not see a select node in a Quokka execution plan shown by `explain()`. 

It is much better to think of a DataStream simply as a stream of rows that meet certain criteria, and who may be non-deterministically 
batched together by the Quokka runtime. Indeed, Quokka makes no guarantees on the sizes of these batches, which is determined at runtime. 
This flexibility is an important reason for Quokka's superior performance.


**Args**

* **columns** (list) : a list of columns to select from the source DataStream

---
Return:
    A DataStream consisting of only the columns selected.


**Examples**

~~~python
>>> f = qc.read_csv("lineitem.csv")

# select only the l_orderdate and l_orderkey columns
>>> f = f.select(["l_orderdate", "l_orderkey"])

# this will now fail, since f's schema now consists of only two columns.
>>> f = f.select(["l_linenumber"])
~~~

### .drop
[source](https://github.com/blob/master/datastream.py/#L314)
```python
.drop(
   cols_to_drop: list
)
```

---
Think of this as the anti-opereator to select. Instead of selecting columns, this will drop columns. 
This is implemented in Quokka as selecting the columns in the DataStream's schema that are not dropped.


**Args**

* **cols_to_drop** (list) : a list of columns to drop from the source DataStream

---
Return:
    A DataStream consisting of all columns in the source DataStream that are not in `cols_to_drop`.


**Examples**

~~~python
>>> f = qc.read_csv("lineitem.csv")

# select only the l_orderdate and l_orderkey columns
>>> f = f.drop(["l_orderdate", "l_orderkey"])

# this will now fail, since you dropped l_orderdate
>>> f = f.select(["l_orderdate"])
~~~

### .rename
[source](https://github.com/blob/master/datastream.py/#L342)
```python
.rename(
   rename_dict
)
```

---
Renames columns in the DataStream according to rename_dict. This is similar to 
[`polars.rename`](https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.DataFrame.rename.html).
The keys you supply in rename_dict must be present in the schema, and the rename operation
must not lead to duplicate column names.

Note this will lead to a physical operation at runtime. 


**Args**

* **rename_dict** (dict) : key is old column name, value is new column name.

---
Return:
    A DataStream with new schema according to rename. 

### .transform
[source](https://github.com/blob/master/datastream.py/#L391)
```python
.transform(
   f, new_schema: list, required_columns: set, foldable = True
)
```

---
This is a rather Quokka-specific API that allows arbitrary transformations on a DataStream, similar to Spark RDD.map.
Each batch in the DataStream is going to be transformed according to a user defined function, which can produce a new batch.
The new batch can have completely different schema or even length as the original batch, and the original data is considered lost,
or consumed by this transformation function. This could be used to implement user-defined-aggregation-functions (UDAFs). Note in
cases where you are simply generating a new column from other columns for each row, i.e. UDF, you probably want to use the 
`with_column` method instead. 

A DataStream is implemented as a stream of batches. In the runtime, your transformation function will be applied to each of those batches.
However, there are no guarantees whatsoever on the sizes of these batches! You should probably make sure your logic is correct
regardless of the sizes of the batches. For example, if your DataStream consists of a column of numbers, and you wish to compute the sum
of those numbers, you could first transform the DataStream to return just the sum of each batch, and then hook this DataStream up to 
a stateful operator that adds up all the sums. 

You can use whatever libraries you have installed in your Python environment in this transformation function. If you are using this on a
cloud cluster, you have to make sure the necessary libraries are installed on each machine. You can use the `utils` package in pyquokka to help
you do this.

This is very similar to Spark's seldom used `combineByKey` feature. 

Note a transformation in the logical plan basically precludes any predicate pushdown or early projection past it, since the original columns 
are assumed to be lost, and we cannot directly establish correspendences between the input columns to a transformation and its output 
columns for the purposes of predicate pushdown or early projection. The user is required to supply a set or list of required columns,
and we will select for those columns (which can be pushed down) before we apply the transformation. 


**Args**

* **f** (function) : The transformation function. This transformation function must take as input a Polars DataFrame and output a Polars DataFrame. 
    The transformation function must not have expectations on the length of its input. Similarly, the transformation function does not 
    have to emit outputs of a specific size. The transformation function must produce the same output columns for every possible input.
* **new_schema** (list) : The names of the columns of the Polars DataFrame that the transformation function produces. 
* **required_columns** (list or set) : The names of the columns that are required for this transformation. This argument is made mandatory
    because it's often trivial to supply and can often greatly speed things up.
* **foldable** (bool) : Whether or not the transformation can be executed as part of the batch post-processing of the previous operation in the 
    execution graph. This is set to True by default. Correctly setting this flag requires some insight into how Quokka works. Lightweight
    functions generally benefit from being folded. Heavyweight functions or those whose efficiency improve with large input sizes 
    might benefit from not being folded. 

---
Return:
    A new transformed DataStream with the supplied schema.


**Examples**

~~~python

# a user defined function that takes in a Polars DataFrame with a single column "text", converts it to a Pyarrow table,
# and uses nice Pyarrow compute functions to perform the word count on this Polars DataFrame. Note 1) we have to convert it 
# back to a Polars DataFrame afterwards, 2) the function works regardless of input length and 3) the output columns are the 
# same regardless of the input.
def udf2(x):
    x = x.to_arrow()
    da = compute.list_flatten(compute.ascii_split_whitespace(x["text"]))
    c = da.value_counts().flatten()
    return polars.from_arrow(pa.Table.from_arrays([c[0], c[1]], names=["word","count"]))

# this is a trick to read in text files, just use read_csv with a separator you know won't appear.
# the result will just be DataStream with one column. 
>>> words = qc.read_csv("random_words.txt", ["text"], sep = "|")

# transform words to counts
>>> counted = words.transform( udf2, new_schema = ["word", "count"], required_columns = {"text"}, foldable=True)
~~~

### .with_column
[source](https://github.com/blob/master/datastream.py/#L474)
```python
.with_column(
   new_column, f, required_columns = None, foldable = True
)
```

---
This will create new columns from certain columns in the dataframe. This is similar to pandas `df.apply()` that makes new columns. 
This is similar to Spark UDF or Pandas UDF, Polars `with_column`, Spark `with_column`, etc. Note that this function, like most Quokka DataStream
functions, are not in-place, and will return a new DataStream, with the new column.

This is a separate API from `transform` because the semantics allow for projection and predicate pushdown through this node, 
since the original columns are all preserved. Use this instead of `transform` if possible.

A DataStream is implemented as a stream of batches. In the runtime, your function will be applied to each of those batches. The function must
take as input a Polars DataFrame and produce a Polars DataFrame. This is a different mental model from say Pandas `df.apply`, where the function is written
for each row. There are two restrictions. First, your result must only have one column, and it should have 
the same name as your `new_column` argument. Second, your result must have the same length as the input Polars DataFrame. 

You can use whatever libraries you have installed in your Python environment in this function. If you are using this on a
cloud cluster, you have to make sure the necessary libraries are installed on each machine. You can use the `utils` package in pyquokka to help
you do this.

Importantly, your function can take full advantage of Polars' columnar APIs to make use of SIMD and other forms of speedy goodness. 
You can even use Polars LazyFrame abstractions inside of this function. Of course, for ultimate flexbility, you are more than welcome to convert 
the Polars DataFrame to a Pandas DataFrame and use `df.apply`. Just remember to convert it back to a Polars DataFrame with only the result column in the end!



**Args**

* **new_column** (str) : The name of the new column.
* **f** (function) : The apply function. This apply function must take as input a Polars DataFrame and output a Polars DataFrame. 
    The apply function must not have expectations on the length of its input. The output must have the same length as the input.
    The apply function must produce the same output columns for every possible input.
* **required_columns** (list or set) : The names of the columns that are required for your function. If this is not specified then Quokka assumes 
    all the columns are required for your function. Early projection past this function becomes impossible. Long story short, if you can 
    specify this argument, do it.
* **foldable** (bool) : Whether or not the function can be executed as part of the batch post-processing of the previous operation in the 
    execution graph. This is set to True by default. Correctly setting this flag requires some insight into how Quokka works. Lightweight
    functions generally benefit from being folded. Heavyweight functions or those whose efficiency improve with large input sizes 
    might benefit from not being folded. 

---
Return:
    A new DataStream with a new column made by the user defined function.


**Examples**


~~~python

>>> f = qc.read_csv("lineitem.csv")

# people who care about speed of execution make full use of Polars columnar APIs.

>>> d = d.with_column("high", lambda x:(x["o_orderpriority"] == "1-URGENT") | (x["o_orderpriority"] == "2-HIGH"), required_columns = {"o_orderpriority"})

# people who care about speed of development can do something that hurts my eyes.

def f(x):
    y = x.to_pandas()
    y["high"] = y.apply(lambda x:(x["o_orderpriority"] == "1-URGENT") | (x["o_orderpriority"] == "2-HIGH"), axis = 1)
    return polars.from_pandas(y["high"])

>>> d = d.with_column("high", f, required_columns={"o_orderpriority"})
~~~

### .stateful_transform
[source](https://github.com/blob/master/datastream.py/#L557)
```python
.stateful_transform(
   executor: Executor, new_schema: list, required_columns: set,
   partitioner = PassThroughPartitioner(), placement = 'cpu'
)
```

---
**EXPERIMENTAL API** 

This is like `transform`, except you can use a stateful object as your transformation function. This is useful for example, if you want to run
a heavy Pytorch model on each batch coming in, and you don't want to reload this model for each function call. Remember the `transform` API only
supports stateless transformations. You could also implement much more complicated stateful transformations, like implementing your own aggregation
function if you are not satisfied with Quokka's default operator's performance.

This API is still being finalized. A version of it that takes multiple input streams is also going to be added. This is the part of the DataStream level 
api that is closest to the underlying execution engine. Quokka's underlying execution engine basically executes a series of stateful transformations
on batches of data. The difficulty here is how much of that underlying API to expose here so it's still useful without the user having to understand 
how the Quokka runtime works. To that end, we have to come up with suitable partitioner and placement strategy abstraction classes and interfaces.

If you are interested in helping us hammer out this API, please talke to me: zihengw@stanford.edu.


**Args**

* **executor** (pyquokka.executors.Executor) : The stateful executor. It must be a subclass of `pyquokka.executors.Executor`, and expose the `execute` 
    and `done` functions. More details forthcoming.
* **new_schema** (list) : The names of the columns of the Polars DataFrame that the transformation function produces. 
* **required_columns** (list or set) : The names of the columns that are required for this transformation. This argument is made mandatory
    because it's often trivial to supply and can often greatly speed things up.

---
Return:
    A transformed DataStream.


**Examples**

Forthcoming.

### .distinct
[source](https://github.com/blob/master/datastream.py/#L617)
```python
.distinct(
   keys: list
)
```

---
Return a new DataStream with specified columns and unique rows. This is like `SELECT DISTINCT(KEYS) FROM ...` in SQL.

Note all the other columns will be dropped, since their behavior is unspecified. If you want to do deduplication, you can use
this operator with keys set to all the columns.

This could be accomplished by using `groupby().agg()` but using `distinct` is generally faster because it is nonblocking, 
compared to a groupby. Quokka really likes nonblocking operations because it can then pipeline it with other operators.


**Args**

* **keys** (list) : a list of columns to select distinct on.

---
Return:
    A transformed DataStream whose columns are in keys and whose rows are unique.


**Examples**

~~~python
>>> f = qc.read_csv("lineitem.csv")

# select only the l_orderdate and l_orderkey columns, return only unique rows.
>>> f = f.distinct(["l_orderdate", "l_orderkey"])

# this will now fail, since l_comment is no longer in f's schema.
>>> f = f.select(["l_comment"])
~~~

### .join
[source](https://github.com/blob/master/datastream.py/#L668)
```python
.join(
   right, on = None, left_on = None, right_on = None, suffix = '_2', how = 'inner'
)
```

---
Join a DataStream with another DataStream or a **small** Polars DataFrame (<10MB). If you have a Polars DataFrame bigger
than this, the best solution right now is to write it out to a file and have Quokka read it back in as a DataStream. I 
realize this is perhaps suboptimal, and this will be improved.

A streaming two-sided distributed join will be executed for two DataStream joins and a streaming broadcast join
will be executed for DataStream joined with Polars DataFrame. Joins are obviously very important, and we are constantly improving
how we do joins. Eventually we will support out of core joins, when @savebuffer merges his PR into Arrow 10.0.


**Args**

* **right** (DataStream or Polars DataFrame) : the DataStream or Polars DataFrame to join to.
* **on** (str) : You could either specify this, if the join column has the same name in this DataStream and `right`, or `left_on` and `right_on` 
    if the join columns don't have the same name.
* **left_on** (str) : the name of the join column in this DataStream.
* **right_on** (str) : the name of the join column in `right`.
* **suffix** (str) : if `right` has columns with the same names as columns in this DataStream, their names will be appended with the suffix in the result.
* **how** (str) : only supports "inner" for now. 

---
Return:
    A new DataStream that's the joined result of this DataStream and "right". By default, columns from both side will be retained, 
    except for `right_on` from the right side. 


**Examples**

~~~python
>>> lineitem = qc.read_csv("lineitem.csv")

>>> orders = qc.read_csv("orders.csv")

>>> result = lineitem.join(orders, left_on = "l_orderkey", right_on = "o_orderkey")

# this will now fail, since o_orderkey is not in the joined DataStream.
>>> result = result.select(["o_orderkey"])
~~~

### .groupby
[source](https://github.com/blob/master/datastream.py/#L775)
```python
.groupby(
   groupby: list, orderby = None
)
```

---
Group a DataStream on a list of columns, optionally specifying an ordering requirement.

This returns a GroupedDataStream object, which currently only expose the `aggregate` method. This is similar to Pandas `df.groupby().agg()` syntax.
Eventually the GroupedDataStream object will also support different kinds of window functions. 


**Args**

* **groupby** (list or str) : a column or a list of columns to group on.
* **orderby** (list) : a list of ordering requirements of the groupby columns, specified in a list like this:
    [(col1, "asc"), (col2, "desc")]. 

---
Return:
    A GroupedDataStream object with the specified grouping and the current DataStream.


**Examples**

~~~python
>>> lineitem = qc.read_csv("lineitem.csv")

>>> result = lineitem.groupby(["l_orderkey","l_orderdate"], orderby = [("l_orderkey", "asc"), ("l_orderdate", "desc")])
~~~

### .agg
[source](https://github.com/blob/master/datastream.py/#L932)
```python
.agg(
   aggregations
)
```

---
Aggregate this DataStream according to the defined aggregations without any pre-grouping. This is similar to Pandas `df.agg()`.
The result will be one row.

The result is a DataStream that will return a batch when the entire aggregation is done, since it's impossible to return any aggregation
results without seeing the entire dataset. As a result, you should call `.compute()` or `.collect()` on this DataStream instead of doing 
additional operations on it like `.filter()` since those won't be pipelined anyways. The only reason Quokka by default returns a DataStream
instead of just returning a Polars DataFrame or a Quokka DataSet is so you can do `.explain()` on it.


**Args**

* **aggregations** (dict) : similar to a dictionary argument to Pandas `df.agg()`. The key is the column name, where the value
    is a str that is "min", "max", "mean", "sum", "avg" or a list of such strings. If you desire to have the count column
    in your result, add a key "*" with value "count". Look at the examples.

---
Return:
    A DataStream object that holds the aggregation result. It will only emit one batch, which is the result when it's done. 
    You should call `.collect()` or `.compute()` on it as it is impossible to pipeline past an 
    aggregation, so might as well as materialize it right now.


**Examples**

~~~python
>>> lineitem = qc.read_csv("lineitem.csv")

>>> d = lineitem.filter("l_shipdate <= date '1998-12-01' - interval '90' day")

>>> d = d.with_column("disc_price", lambda x:x["l_extendedprice"] * (1 - x["l_discount"]), required_columns ={"l_extendedprice", "l_discount"})

# I want the sum and average of the l_quantity column and the l_extendedprice colum, the sum of the disc_price column, the minimum of the l_discount
# column, and oh give me the total row count as well.
>>> f = d.agg({"l_quantity":["sum","avg"], "l_extendedprice":["sum","avg"], "disc_price":"sum", "l_discount":"min","*":"count"})
~~~

### .aggregate
[source](https://github.com/blob/master/datastream.py/#L969)
```python
.aggregate(
   aggregations
)
```

---
Alias of `agg`.

### .count
[source](https://github.com/blob/master/datastream.py/#L977)
```python
.count()
```

---
Return total row count.

### .sum
[source](https://github.com/blob/master/datastream.py/#L985)
```python
.sum(
   columns
)
```

---
Return the sums of the specified columns.

### .max
[source](https://github.com/blob/master/datastream.py/#L998)
```python
.max(
   columns
)
```

---
Return the maximum values of the specified columns.

### .min
[source](https://github.com/blob/master/datastream.py/#L1011)
```python
.min(
   columns
)
```

---
Return the minimum values of the specified columns.

### .mean
[source](https://github.com/blob/master/datastream.py/#L1024)
```python
.mean(
   columns
)
```

---
Return the mean values of the specified columns.

----


## GroupedDataStream
[source](https://github.com/blob/master/datastream.py/#L1038)
```python 
GroupedDataStream(
   source_data_stream: DataStream, groupby, orderby
)
```




**Methods:**


### .agg
[source](https://github.com/blob/master/datastream.py/#L1045)
```python
.agg(
   aggregations: dict
)
```

---
Aggregate this GroupedDataStream according to the defined aggregations. This is similar to Pandas `df.groupby().agg()`.
The result's length will be however number of rows as there are unique group keys combinations.

The result is a DataStream that will return a batch when the entire aggregation is done, since it's impossible to return any aggregation
results without seeing the entire dataset. As a result, you should call `.compute()` or `.collect()` on this DataStream instead of doing 
additional operations on it like `.filter()` since those won't be pipelined anyways. The only reason Quokka by default returns a DataStream
instead of just returning a Polars DataFrame or a Quokka DataSet is so you can do `.explain()` on it.


**Args**

* **aggregations** (dict) : similar to a dictionary argument to Pandas `df.agg()`. The key is the column name, where the value
    is a str that is "min", "max", "mean", "sum", "avg" or a list of such strings. If you desire to have the count column
    in your result, add a key "*" with value "count". Look at the examples.

---
Return:
    A DataStream object that holds the aggregation result. It will only emit one batch, which is the result when it's done. 
    You should call `.collect()` or `.compute()` on it as it is impossible to pipeline past an 
    aggregation, so might as well as materialize it right now.


**Examples**

~~~python
>>> lineitem = qc.read_csv("lineitem.csv")

>>> d = lineitem.filter("l_shipdate <= date '1998-12-01' - interval '90' day")

>>> d = d.with_column("disc_price", lambda x:x["l_extendedprice"] * (1 - x["l_discount"]), required_columns ={"l_extendedprice", "l_discount"})

# I want the sum and average of the l_quantity column and the l_extendedprice colum, the sum of the disc_price column, the minimum of the l_discount
# column, and oh give me the total row count as well, of each unique combination of l_returnflag and l_linestatus
>>> f = d.groupby(["l_returnflag", "l_linestatus"]).agg({"l_quantity":["sum","avg"], "l_extendedprice":["sum","avg"], "disc_price":"sum", "l_discount":"min","*":"count"})
~~~

### .aggregate
[source](https://github.com/blob/master/datastream.py/#L1082)
```python
.aggregate(
   aggregations: dict
)
```

---
Alias for agg.
