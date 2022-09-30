#


## TargetInfo
[source](https://github.com/blob/master/target_info.py/#L5)
```python 
TargetInfo(
   partitioner, predicate: sqlglot.Expression, projection, batch_funcs: list
)
```




**Methods:**


### .and_predicate
[source](https://github.com/blob/master/target_info.py/#L20)
```python
.and_predicate(
   predicate
)
```


### .predicate_required_columns
[source](https://github.com/blob/master/target_info.py/#L24)
```python
.predicate_required_columns()
```


### .append_batch_func
[source](https://github.com/blob/master/target_info.py/#L27)
```python
.append_batch_func(
   f
)
```


----


## Partitioner
[source](https://github.com/blob/master/target_info.py/#L34)
```python 

```



----


## PassThroughPartitioner
[source](https://github.com/blob/master/target_info.py/#L38)
```python 

```



----


## BroadcastPartitioner
[source](https://github.com/blob/master/target_info.py/#L44)
```python 

```



----


## HashPartitioner
[source](https://github.com/blob/master/target_info.py/#L50)
```python 
HashPartitioner(
   key
)
```



----


## FunctionPartitioner
[source](https://github.com/blob/master/target_info.py/#L57)
```python 
FunctionPartitioner(
   func
)
```


