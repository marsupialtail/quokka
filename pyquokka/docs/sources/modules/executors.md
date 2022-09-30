#


## Executor
[source](https://github.com/blob/master/executors.py/#L18)
```python 

```




**Methods:**


### .execute
[source](https://github.com/blob/master/executors.py/#L21)
```python
.execute(
   batches, stream_id, executor_id
)
```


### .done
[source](https://github.com/blob/master/executors.py/#L23)
```python
.done(
   executor_id
)
```


----


## UDFExecutor
[source](https://github.com/blob/master/executors.py/#L26)
```python 
UDFExecutor(
   udf
)
```




**Methods:**


### .serialize
[source](https://github.com/blob/master/executors.py/#L30)
```python
.serialize()
```


### .deserialize
[source](https://github.com/blob/master/executors.py/#L33)
```python
.deserialize(
   s
)
```


### .execute
[source](https://github.com/blob/master/executors.py/#L36)
```python
.execute(
   batches, stream_id, executor_id
)
```


### .done
[source](https://github.com/blob/master/executors.py/#L43)
```python
.done(
   executor_id
)
```


----


## StorageExecutor
[source](https://github.com/blob/master/executors.py/#L47)
```python 

```




**Methods:**


### .serialize
[source](https://github.com/blob/master/executors.py/#L50)
```python
.serialize()
```


### .deserialize
[source](https://github.com/blob/master/executors.py/#L52)
```python
.deserialize(
   s
)
```


### .execute
[source](https://github.com/blob/master/executors.py/#L55)
```python
.execute(
   batches, stream_id, executor_id
)
```


### .done
[source](https://github.com/blob/master/executors.py/#L64)
```python
.done(
   executor_id
)
```


----


## OutputExecutor
[source](https://github.com/blob/master/executors.py/#L68)
```python 
OutputExecutor(
   filepath, format, prefix = 'part', mode = 'local', row_group_size = 5000000
)
```




**Methods:**


### .serialize
[source](https://github.com/blob/master/executors.py/#L80)
```python
.serialize()
```


### .deserialize
[source](https://github.com/blob/master/executors.py/#L83)
```python
.deserialize(
   s
)
```


### .execute
[source](https://github.com/blob/master/executors.py/#L86)
```python
.execute(
   batches, stream_id, executor_id
)
```


### .done
[source](https://github.com/blob/master/executors.py/#L135)
```python
.done(
   executor_id
)
```


----


## BroadcastJoinExecutor
[source](https://github.com/blob/master/executors.py/#L153)
```python 
BroadcastJoinExecutor(
   small_table, on = None, small_on = None, big_on = None, suffix = '_small',
   how = 'inner'
)
```




**Methods:**


### .serialize
[source](https://github.com/blob/master/executors.py/#L182)
```python
.serialize()
```


### .deserialize
[source](https://github.com/blob/master/executors.py/#L193)
```python
.deserialize(
   s
)
```


### .execute
[source](https://github.com/blob/master/executors.py/#L198)
```python
.execute(
   batches, stream_id, executor_id
)
```


### .done
[source](https://github.com/blob/master/executors.py/#L209)
```python
.done(
   executor_id
)
```


----


## GroupAsOfJoinExecutor
[source](https://github.com/blob/master/executors.py/#L215)
```python 
GroupAsOfJoinExecutor(
   group_on = None, group_left_on = None, group_right_on = None, on = None,
   left_on = None, right_on = None, suffix = '_right'
)
```




**Methods:**


### .serialize
[source](https://github.com/blob/master/executors.py/#L244)
```python
.serialize()
```


### .deserialize
[source](https://github.com/blob/master/executors.py/#L248)
```python
.deserialize(
   s
)
```


### .find_second_smallest
[source](https://github.com/blob/master/executors.py/#L253)
```python
.find_second_smallest(
   batch, key
)
```


### .execute
[source](https://github.com/blob/master/executors.py/#L260)
```python
.execute(
   batches, stream_id, executor_id
)
```


### .done
[source](https://github.com/blob/master/executors.py/#L390)
```python
.done(
   executor_id
)
```


----


## PolarJoinExecutor
[source](https://github.com/blob/master/executors.py/#L404)
```python 
PolarJoinExecutor(
   on = None, left_on = None, right_on = None, suffix = '_right', how = 'inner'
)
```




**Methods:**


### .serialize
[source](https://github.com/blob/master/executors.py/#L426)
```python
.serialize()
```


### .deserialize
[source](https://github.com/blob/master/executors.py/#L434)
```python
.deserialize(
   s
)
```


### .execute
[source](https://github.com/blob/master/executors.py/#L444)
```python
.execute(
   batches, stream_id, executor_id
)
```


### .done
[source](https://github.com/blob/master/executors.py/#L474)
```python
.done(
   executor_id
)
```


----


## DistinctExecutor
[source](https://github.com/blob/master/executors.py/#L479)
```python 
DistinctExecutor(
   keys
)
```




**Methods:**


### .execute
[source](https://github.com/blob/master/executors.py/#L485)
```python
.execute(
   batches, stream_id, executor_id
)
```


### .serialize
[source](https://github.com/blob/master/executors.py/#L501)
```python
.serialize()
```


### .deserialize
[source](https://github.com/blob/master/executors.py/#L504)
```python
.deserialize(
   s
)
```


### .done
[source](https://github.com/blob/master/executors.py/#L509)
```python
.done(
   executor_id
)
```


----


## AggExecutor
[source](https://github.com/blob/master/executors.py/#L512)
```python 
AggExecutor(
   groupby_keys, orderby_keys, aggregation_dict, mean_cols, count
)
```


---
aggregation_dict will define what you are going to do for


**Methods:**


### .serialize
[source](https://github.com/blob/master/executors.py/#L548)
```python
.serialize()
```


### .deserialize
[source](https://github.com/blob/master/executors.py/#L551)
```python
.deserialize(
   s
)
```


### .execute
[source](https://github.com/blob/master/executors.py/#L557)
```python
.execute(
   batches, stream_id, executor_id
)
```


### .done
[source](https://github.com/blob/master/executors.py/#L572)
```python
.done(
   executor_id
)
```


----


## LimitExecutor
[source](https://github.com/blob/master/executors.py/#L600)
```python 
LimitExecutor(
   limit
)
```




**Methods:**


### .execute
[source](https://github.com/blob/master/executors.py/#L605)
```python
.execute(
   batches, stream_id, executor_id
)
```


### .done
[source](https://github.com/blob/master/executors.py/#L613)
```python
.done()
```


----


## CountExecutor
[source](https://github.com/blob/master/executors.py/#L616)
```python 

```




**Methods:**


### .execute
[source](https://github.com/blob/master/executors.py/#L621)
```python
.execute(
   batches, stream_id, executor_id
)
```


### .serialize
[source](https://github.com/blob/master/executors.py/#L625)
```python
.serialize()
```


### .deserialize
[source](https://github.com/blob/master/executors.py/#L628)
```python
.deserialize(
   s
)
```


### .done
[source](https://github.com/blob/master/executors.py/#L633)
```python
.done(
   executor_id
)
```


----


## MergeSortedExecutor
[source](https://github.com/blob/master/executors.py/#L638)
```python 
MergeSortedExecutor(
   key, record_batch_rows = None, length_limit = 5000, file_prefix = 'mergesort'
)
```




**Methods:**


### .serialize
[source](https://github.com/blob/master/executors.py/#L651)
```python
.serialize()
```


### .deserialize
[source](https://github.com/blob/master/executors.py/#L654)
```python
.deserialize(
   s
)
```


### .write_out_df_to_disk
[source](https://github.com/blob/master/executors.py/#L657)
```python
.write_out_df_to_disk(
   target_filepath, input_mem_table
)
```


### .produce_sorted_file_from_two_sorted_files
[source](https://github.com/blob/master/executors.py/#L666)
```python
.produce_sorted_file_from_two_sorted_files(
   target_filepath, input_filepath1, input_filepath2
)
```


### .done
[source](https://github.com/blob/master/executors.py/#L737)
```python
.done(
   executor_id
)
```


### .execute
[source](https://github.com/blob/master/executors.py/#L755)
```python
.execute(
   batches, stream_id, executor_id
)
```

