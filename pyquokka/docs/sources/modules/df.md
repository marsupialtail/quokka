#


## QuokkaContext
[source](https://github.com/blob/master/df.py/#L9)
```python 
QuokkaContext(
   cluster = None
)
```




**Methods:**


### .read_files
[source](https://github.com/blob/master/df.py/#L15)
```python
.read_files(
   table_location: str
)
```

---
This doesn't work yet due to difficulty handling Object types in Polars

### .read_csv
[source](https://github.com/blob/master/df.py/#L66)
```python
.read_csv(
   table_location: str, schema = None, has_header = False, sep = ', '
)
```

---
Read in a CSV file or files from a table location. It can be a single CSV or a list of CSVs. It can be CSV(s) on disk
or CSV(s) on S3. Currently other cloud sare not supported. The CSVs can have a predefined schema using a list of 
column names in the schema argument, or you can specify the CSV has a header row and Quokka will read the schema 
from it. You should also specify the CSV's separator. 


**Args**

* **table_location** (str) : where the CSV(s) are. This mostly mimics Spark behavior. Look at the examples.
* **schema** (list) : you can provide a list of column names, it's kinda like polars.read_csv(new_columns=...)
* **has_header** (bool) : is there a header row. If the schema is not provided, this should be True. If the schema IS provided, 
    this can still be True. Quokka will just ignore the header row.
* **sep** (str) : default to ',' but could be something else, like '|' for TPC-H tables

---
Return:
    A new DataStream if the CSV file is larger than 10MB, otherwise a Polars DataFrame. 


**Examples**

~~~python
# read a single CSV. It's better always to specify the absolute path.
>>> lineitem = qc.read_csv("/home/ubuntu/tpch/lineitem.csv")

# read a directory of CSVs 
>>> lineitem = qc.read_csv("/home/ubuntu/tpch/lineitem/*")

# read a single CSV from S3
>>> lineitem = qc.read_csv("s3://tpc-h-csv/lineitem/lineitem.tbl.1")

# read CSVs from S3 bucket with prefix
>>> lineitem = qc.read_csv("s3://tpc-h-csv/lineitem/*")
~~~

### .read_parquet
[source](https://github.com/blob/master/df.py/#L209)
```python
.read_parquet(
   table_location: str, schema = None
)
```

---
Read Parquet. It can be a single Parquet or a list of Parquets. It can be Parquet(s) on disk
or Parquet(s) on S3. Currently other cloud sare not supported. You don't really have to supply the schema
since you can get it from the metadata always, but you can if you want.


**Args**

* **table_location** (str) : where the Parquet(s) are. This mostly mimics Spark behavior. Look at the examples.
* **schema** (list) : list of column names. This is optional. If you do supply it, please make sure it's correct!

---
Return:
    A new DataStream if the Parquet file is larger than 10MB, otherwise a Polars DataFrame. 


**Examples**

~~~python
# read a single Parquet. It's better always to specify the absolute path.
>>> lineitem = qc.read_parquet("/home/ubuntu/tpch/lineitem.parquet")

# read a directory of Parquets 
>>> lineitem = qc.read_parquet("/home/ubuntu/tpch/lineitem/*")

# read a single Parquet from S3
>>> lineitem = qc.read_parquet("s3://tpc-h-parquet/lineitem.parquet")

# read Parquets from S3 bucket with prefix
>>> lineitem = qc.read_parquet("s3://tpc-h-parquet/lineitem/*")
~~~

### .new_stream
[source](https://github.com/blob/master/df.py/#L340)
```python
.new_stream(
   sources: dict, partitioners: dict, node: Node, schema: list, ordering = None
)
```


### .new_dataset
[source](https://github.com/blob/master/df.py/#L356)
```python
.new_dataset(
   source, schema: list
)
```


### .optimize
[source](https://github.com/blob/master/df.py/#L361)
```python
.optimize(
   node_id
)
```


### .lower
[source](https://github.com/blob/master/df.py/#L379)
```python
.lower(
   end_node_id, collect = True
)
```


### .execute_node
[source](https://github.com/blob/master/df.py/#L439)
```python
.execute_node(
   node_id, explain = False, mode = None, collect = True
)
```


### .explain
[source](https://github.com/blob/master/df.py/#L478)
```python
.explain(
   node_id, mode = 'graph'
)
```


----


## DataSet
[source](https://github.com/blob/master/df.py/#L764)
```python 
DataSet(
   quokka_context: QuokkaContext, schema: dict, source_node_id: int
)
```


