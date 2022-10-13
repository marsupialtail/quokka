#


## QuokkaContext
[source](https://github.com/blob/master/df.py/#L9)
```python 
QuokkaContext(
   cluster = None
)
```

---
Initialize a `QuokkaContext` object. This is similar to Spark's `SQLContext`. It is responsible for registering all the data sources etc. and planning your execution. It is initialized by a cluster argument, which defaults to `LocalCluster()`. If you want to use EC2, you need to create and `EC2Cluster` first with the tools in `pyquokka.utils` and then make a `QuokkaContext` with an `EC2Cluster`.

**Args**

* **cluster**: if None, will create a `LocalCluster` instance to execute locally. You can also provide an `EC2Cluster`. Please refer to [this page](cloud.md) for help.

**Methods:**

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