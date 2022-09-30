#


## DataStream
[source](https://github.com/blob/master/datastream.py/#L11)
```python 
DataStream(
   quokka_context, schema: list, source_node_id: int
)
```




**Methods:**


### .collect
[source](https://github.com/blob/master/datastream.py/#L17)
```python
.collect()
```

---
This will trigger the execution of computational graph, similar to Spark collect
The result will be a Polars DataFrame on the master

### .compute
[source](https://github.com/blob/master/datastream.py/#L25)
```python
.compute()
```

---
This will trigger the execution of computational graph, similar to Spark collect
The result will be a Dataset, which you can then call to_df() or call to_stream() to initiate another computation.

### .explain
[source](https://github.com/blob/master/datastream.py/#L33)
```python
.explain(
   mode = 'graph'
)
```

---
This will not trigger the execution of your computation graph but will produce a graph of the execution plan.

### .write_csv
[source](https://github.com/blob/master/datastream.py/#L40)
```python
.write_csv(
   table_location, output_line_limit = 1000000
)
```


### .write_parquet
[source](https://github.com/blob/master/datastream.py/#L76)
```python
.write_parquet(
   table_location, output_line_limit = 10000000
)
```


### .filter
[source](https://github.com/blob/master/datastream.py/#L110)
```python
.filter(
   predicate: str
)
```


### .select
[source](https://github.com/blob/master/datastream.py/#L123)
```python
.select(
   columns: list
)
```


### .drop
[source](https://github.com/blob/master/datastream.py/#L137)
```python
.drop(
   cols_to_drop: list
)
```


### .rename
[source](https://github.com/blob/master/datastream.py/#L143)
```python
.rename(
   rename_dict
)
```


### .transform
[source](https://github.com/blob/master/datastream.py/#L187)
```python
.transform(
   f, new_schema: list, required_columns: set, foldable = True
)
```


### .with_column
[source](https://github.com/blob/master/datastream.py/#L215)
```python
.with_column(
   new_column, f, required_columns = None, foldable = True, engine = 'polars'
)
```


### .stateful_transform
[source](https://github.com/blob/master/datastream.py/#L248)
```python
.stateful_transform(
   executor: Executor, new_schema: list, required_columns: set,
   partitioner = PassThroughPartitioner(), placement = 'cpu'
)
```


### .distinct
[source](https://github.com/blob/master/datastream.py/#L281)
```python
.distinct(
   keys: list
)
```


### .join
[source](https://github.com/blob/master/datastream.py/#L305)
```python
.join(
   right, on = None, left_on = None, right_on = None, suffix = '_2', how = 'inner'
)
```


### .groupby
[source](https://github.com/blob/master/datastream.py/#L369)
```python
.groupby(
   groupby: list, orderby = None
)
```


### .agg
[source](https://github.com/blob/master/datastream.py/#L493)
```python
.agg(
   aggregations
)
```


### .aggregate
[source](https://github.com/blob/master/datastream.py/#L496)
```python
.aggregate(
   aggregations
)
```


----


## GroupedDataStream
[source](https://github.com/blob/master/datastream.py/#L500)
```python 
GroupedDataStream(
   source_data_stream: DataStream, groupby, orderby
)
```




**Methods:**


### .agg
[source](https://github.com/blob/master/datastream.py/#L511)
```python
.agg(
   aggregations: dict
)
```


### .aggregate
[source](https://github.com/blob/master/datastream.py/#L518)
```python
.aggregate(
   aggregations: dict
)
```

