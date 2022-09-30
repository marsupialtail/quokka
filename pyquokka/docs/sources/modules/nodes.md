#


## FlushedMessage
[source](https://github.com/blob/master/nodes.py/#L61)
```python 
FlushedMessage(
   loc
)
```



----


## SharedMemMessage
[source](https://github.com/blob/master/nodes.py/#L65)
```python 
SharedMemMessage(
   form, name
)
```



----


## Node
[source](https://github.com/blob/master/nodes.py/#L70)
```python 
Node(
   id, channel
)
```




**Methods:**


### .initialize
[source](https://github.com/blob/master/nodes.py/#L96)
```python
.initialize()
```


### .append_to_targets
[source](https://github.com/blob/master/nodes.py/#L99)
```python
.append_to_targets(
   tup
)
```


### .update_targets
[source](https://github.com/blob/master/nodes.py/#L128)
```python
.update_targets()
```


### .push
[source](https://github.com/blob/master/nodes.py/#L150)
```python
.push(
   data
)
```

---
Quokka should have some support for custom data types, similar to DaFt by Eventual.
Since we transmit data through Arrow Flight, we need to convert the data into arrow record batches.
All Quokka data readers and executors should take a Polars DataFrame as input batch type and output type.
The other data types are not going to be used that much.

### .done
[source](https://github.com/blob/master/nodes.py/#L254)
```python
.done()
```


----


## InputNode
[source](https://github.com/blob/master/nodes.py/#L282)
```python 
InputNode(
   id, channel
)
```




**Methods:**


### .ping
[source](https://github.com/blob/master/nodes.py/#L292)
```python
.ping()
```


### .initialize
[source](https://github.com/blob/master/nodes.py/#L295)
```python
.initialize()
```


### .execute
[source](https://github.com/blob/master/nodes.py/#L298)
```python
.execute()
```


----


## TaskNode
[source](https://github.com/blob/master/nodes.py/#L336)
```python 
TaskNode(
   id, channel, mapping, functionObject, parents
)
```




**Methods:**


### .initialize
[source](https://github.com/blob/master/nodes.py/#L352)
```python
.initialize()
```


### .get_batches
[source](https://github.com/blob/master/nodes.py/#L358)
```python
.get_batches()
```


### .schedule_for_execution
[source](https://github.com/blob/master/nodes.py/#L368)
```python
.schedule_for_execution(
   batch_info
)
```


----


### convert_to_format
[source](https://github.com/blob/master/nodes.py/#L23)
```python
.convert_to_format(
   batch, format
)
```


----


### convert_from_format
[source](https://github.com/blob/master/nodes.py/#L39)
```python
.convert_from_format(
   payload
)
```

