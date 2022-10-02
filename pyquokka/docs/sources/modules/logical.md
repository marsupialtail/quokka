#


## PlacementStrategy
[source](https://github.com/blob/master/logical.py/#L6)
```python 

```



----


## SingleChannelStrategy
[source](https://github.com/blob/master/logical.py/#L13)
```python 

```



----


## CustomChannelsStrategy
[source](https://github.com/blob/master/logical.py/#L20)
```python 
CustomChannelsStrategy(
   channels
)
```



----


## GPUStrategy
[source](https://github.com/blob/master/logical.py/#L28)
```python 

```



----


## Node
[source](https://github.com/blob/master/logical.py/#L41)
```python 
Node(
   schema
)
```




**Methods:**


### .lower
[source](https://github.com/blob/master/logical.py/#L53)
```python
.lower(
   task_graph
)
```


### .set_placement_strategy
[source](https://github.com/blob/master/logical.py/#L56)
```python
.set_placement_strategy(
   strategy
)
```


----


## SourceNode
[source](https://github.com/blob/master/logical.py/#L65)
```python 
SourceNode(
   schema
)
```



----


## InputS3FilesNode
[source](https://github.com/blob/master/logical.py/#L75)
```python 
InputS3FilesNode(
   bucket, prefix, schema
)
```




**Methods:**


### .lower
[source](https://github.com/blob/master/logical.py/#L81)
```python
.lower(
   task_graph, ip_to_num_channel = None
)
```


----


## InputDiskFilesNode
[source](https://github.com/blob/master/logical.py/#L86)
```python 
InputDiskFilesNode(
   directory, schema
)
```




**Methods:**


### .lower
[source](https://github.com/blob/master/logical.py/#L91)
```python
.lower(
   task_graph, ip_to_num_channel = None
)
```


----


## InputS3CSVNode
[source](https://github.com/blob/master/logical.py/#L96)
```python 
InputS3CSVNode(
   bucket, prefix, key, schema, sep, has_header
)
```




**Methods:**


### .lower
[source](https://github.com/blob/master/logical.py/#L105)
```python
.lower(
   task_graph, ip_to_num_channel = None
)
```


----


## InputDiskCSVNode
[source](https://github.com/blob/master/logical.py/#L110)
```python 
InputDiskCSVNode(
   filename, schema, sep, has_header
)
```




**Methods:**


### .lower
[source](https://github.com/blob/master/logical.py/#L117)
```python
.lower(
   task_graph, ip_to_num_channel = None
)
```


----


## InputS3ParquetNode
[source](https://github.com/blob/master/logical.py/#L122)
```python 
InputS3ParquetNode(
   filepath, schema, predicate = None, projection = None
)
```




**Methods:**


### .lower
[source](https://github.com/blob/master/logical.py/#L131)
```python
.lower(
   task_graph, ip_to_num_channel = None
)
```


----


## InputDiskParquetNode
[source](https://github.com/blob/master/logical.py/#L149)
```python 
InputDiskParquetNode(
   filepath, schema, predicate = None, projection = None
)
```




**Methods:**


### .lower
[source](https://github.com/blob/master/logical.py/#L156)
```python
.lower(
   task_graph, ip_to_num_channel = None
)
```


----


## SinkNode
[source](https://github.com/blob/master/logical.py/#L171)
```python 
SinkNode(
   schema
)
```



----


## DataSetNode
[source](https://github.com/blob/master/logical.py/#L175)
```python 
DataSetNode(
   schema
)
```




**Methods:**


### .lower
[source](https://github.com/blob/master/logical.py/#L179)
```python
.lower(
   task_graph, parent_nodes, parent_source_info, ip_to_num_channel = None
)
```


----


## TaskNode
[source](https://github.com/blob/master/logical.py/#L197)
```python 
TaskNode(
   schema: list, schema_mapping: dict, required_columns: set
)
```



----


## StatefulNode
[source](https://github.com/blob/master/logical.py/#L204)
```python 
StatefulNode(
   schema, schema_mapping, required_columns, operator
)
```




**Methods:**


### .lower
[source](https://github.com/blob/master/logical.py/#L209)
```python
.lower(
   task_graph, parent_nodes, parent_source_info, ip_to_num_channel = None
)
```


----


## MapNode
[source](https://github.com/blob/master/logical.py/#L232)
```python 
MapNode(
   schema, schema_mapping, required_columns, function, foldable = True
)
```




**Methods:**


### .lower
[source](https://github.com/blob/master/logical.py/#L238)
```python
.lower(
   task_graph, parent_nodes, parent_source_info, ip_to_num_channel = None
)
```


----


## FilterNode
[source](https://github.com/blob/master/logical.py/#L244)
```python 
FilterNode(
   schema, predicate: sqlglot.Expression
)
```




**Methods:**


### .lower
[source](https://github.com/blob/master/logical.py/#L254)
```python
.lower(
   task_graph, parent_nodes, parent_source_info, ip_to_num_channel = None
)
```


----


## ProjectionNode
[source](https://github.com/blob/master/logical.py/#L258)
```python 
ProjectionNode(
   projection: set
)
```


