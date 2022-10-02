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
[source](https://github.com/blob/master/df.py/#L24)
```python
.read_files(
   table_location: str
)
```


### .read_csv
[source](https://github.com/blob/master/df.py/#L63)
```python
.read_csv(
   table_location: str, schema = None, has_header = False, sep = ', '
)
```


### .read_parquet
[source](https://github.com/blob/master/df.py/#L173)
```python
.read_parquet(
   table_location: str, schema = None
)
```


### .new_stream
[source](https://github.com/blob/master/df.py/#L275)
```python
.new_stream(
   sources: dict, partitioners: dict, node: Node, schema: list, ordering = None
)
```


### .new_dataset
[source](https://github.com/blob/master/df.py/#L291)
```python
.new_dataset(
   source, schema: list
)
```


### .optimize
[source](https://github.com/blob/master/df.py/#L296)
```python
.optimize(
   node_id
)
```


### .lower
[source](https://github.com/blob/master/df.py/#L314)
```python
.lower(
   end_node_id, collect = True
)
```


### .execute_node
[source](https://github.com/blob/master/df.py/#L374)
```python
.execute_node(
   node_id, explain = False, mode = None, collect = True
)
```


### .explain
[source](https://github.com/blob/master/df.py/#L413)
```python
.explain(
   node_id, mode = 'graph'
)
```


----


## DataSet
[source](https://github.com/blob/master/df.py/#L699)
```python 
DataSet(
   quokka_context: QuokkaContext, schema: dict, source_node_id: int
)
```


