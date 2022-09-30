#


## PlacementStrategy
[source](https://github.com/blob/master/logical.py/#L7)
```python 

```



----


## SingleChannelStrategy
[source](https://github.com/blob/master/logical.py/#L14)
```python 

```



----


## CustomChannelsStrategy
[source](https://github.com/blob/master/logical.py/#L21)
```python 
CustomChannelsStrategy(
   channels
)
```



----


## GPUStrategy
[source](https://github.com/blob/master/logical.py/#L29)
```python 

```



----


## Node
[source](https://github.com/blob/master/logical.py/#L42)
```python 
Node(
   schema
)
```




**Methods:**


### .lower
[source](https://github.com/blob/master/logical.py/#L54)
```python
.lower(
   task_graph
)
```


### .set_placement_strategy
[source](https://github.com/blob/master/logical.py/#L57)
```python
.set_placement_strategy(
   strategy
)
```


----


## SourceNode
[source](https://github.com/blob/master/logical.py/#L66)
```python 
SourceNode(
   schema
)
```



----


## InputS3FilesNode
[source](https://github.com/blob/master/logical.py/#L76)
```python 
InputS3FilesNode(
   bucket, prefix, schema
)
```




**Methods:**


### .lower
[source](https://github.com/blob/master/logical.py/#L82)
```python
.lower(
   task_graph, ip_to_num_channel = None
)
```


----


## InputDiskFilesNode
[source](https://github.com/blob/master/logical.py/#L87)
```python 
InputDiskFilesNode(
   directory, schema
)
```




**Methods:**


### .lower
[source](https://github.com/blob/master/logical.py/#L92)
```python
.lower(
   task_graph, ip_to_num_channel = None
)
```


----


## InputS3CSVNode
[source](https://github.com/blob/master/logical.py/#L97)
```python 
InputS3CSVNode(
   bucket, prefix, key, schema, sep, has_header
)
```




**Methods:**


### .lower
[source](https://github.com/blob/master/logical.py/#L106)
```python
.lower(
   task_graph, ip_to_num_channel = None
)
```


----


## InputDiskCSVNode
[source](https://github.com/blob/master/logical.py/#L111)
```python 
InputDiskCSVNode(
   filename, schema, sep, has_header
)
```




**Methods:**


### .lower
[source](https://github.com/blob/master/logical.py/#L118)
```python
.lower(
   task_graph, ip_to_num_channel = None
)
```


----


## InputS3ParquetNode
[source](https://github.com/blob/master/logical.py/#L123)
```python 
InputS3ParquetNode(
   filepath, schema, predicate = None, projection = None
)
```




**Methods:**


### .lower
[source](https://github.com/blob/master/logical.py/#L132)
```python
.lower(
   task_graph, ip_to_num_channel = None
)
```


----


## InputDiskParquetNode
[source](https://github.com/blob/master/logical.py/#L150)
```python 
InputDiskParquetNode(
   filepath, schema, predicate = None, projection = None
)
```




**Methods:**


### .lower
[source](https://github.com/blob/master/logical.py/#L157)
```python
.lower(
   task_graph, ip_to_num_channel = None
)
```


----


## SinkNode
[source](https://github.com/blob/master/logical.py/#L172)
```python 
SinkNode(
   schema
)
```



----


## DataSetNode
[source](https://github.com/blob/master/logical.py/#L176)
```python 
DataSetNode(
   schema
)
```




**Methods:**


### .lower
[source](https://github.com/blob/master/logical.py/#L180)
```python
.lower(
   task_graph, parent_nodes, parent_source_info, ip_to_num_channel = None
)
```


----


## TaskNode
[source](https://github.com/blob/master/logical.py/#L198)
```python 
TaskNode(
   schema: list, schema_mapping: dict, required_columns: set
)
```



----


## StatefulNode
[source](https://github.com/blob/master/logical.py/#L205)
```python 
StatefulNode(
   schema, schema_mapping, required_columns, operator
)
```




**Methods:**


### .lower
[source](https://github.com/blob/master/logical.py/#L210)
```python
.lower(
   task_graph, parent_nodes, parent_source_info, ip_to_num_channel = None
)
```


----


## MapNode
[source](https://github.com/blob/master/logical.py/#L233)
```python 
MapNode(
   schema, schema_mapping, required_columns, function, foldable = True
)
```




**Methods:**


### .lower
[source](https://github.com/blob/master/logical.py/#L239)
```python
.lower()
```


----


## FilterNode
[source](https://github.com/blob/master/logical.py/#L242)
```python 
FilterNode(
   schema, predicate: sqlglot.Expression
)
```



----


## ProjectionNode
[source](https://github.com/blob/master/logical.py/#L252)
```python 
ProjectionNode(
   projection: set
)
```


