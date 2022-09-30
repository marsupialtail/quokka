#


## Dataset
[source](https://github.com/blob/master/quokka_runtime.py/#L20)
```python 
Dataset(
   wrapped_dataset
)
```




**Methods:**


### .to_list
[source](https://github.com/blob/master/quokka_runtime.py/#L25)
```python
.to_list()
```


### .to_df
[source](https://github.com/blob/master/quokka_runtime.py/#L28)
```python
.to_df()
```


### .to_dict
[source](https://github.com/blob/master/quokka_runtime.py/#L31)
```python
.to_dict()
```


----


## TaskGraph
[source](https://github.com/blob/master/quokka_runtime.py/#L108)
```python 
TaskGraph(
   cluster
)
```




**Methods:**


### .flip_ip_channels
[source](https://github.com/blob/master/quokka_runtime.py/#L123)
```python
.flip_ip_channels(
   ip_to_num_channel
)
```


### .epilogue
[source](https://github.com/blob/master/quokka_runtime.py/#L136)
```python
.epilogue(
   tasknode, channel_to_ip, ips
)
```


### .new_input_redis
[source](https://github.com/blob/master/quokka_runtime.py/#L143)
```python
.new_input_redis(
   dataset, ip_to_num_channel = None, policy = 'default'
)
```


### .new_input_reader_node
[source](https://github.com/blob/master/quokka_runtime.py/#L206)
```python
.new_input_reader_node(
   reader, ip_to_num_channel = None
)
```


### .flip_channels_ip
[source](https://github.com/blob/master/quokka_runtime.py/#L240)
```python
.flip_channels_ip(
   channel_to_ip
)
```


### .get_default_partition
[source](https://github.com/blob/master/quokka_runtime.py/#L247)
```python
.get_default_partition(
   source_ip_to_num_channel, target_ip_to_num_channel
)
```


### .prologue
[source](https://github.com/blob/master/quokka_runtime.py/#L284)
```python
.prologue(
   streams, ip_to_num_channel, channel_to_ip, source_target_info
)
```

---
Remember, the partition key is a function. It is executed on an output batch after the predicate and projection but before the batch funcs.

### .new_non_blocking_node
[source](https://github.com/blob/master/quokka_runtime.py/#L363)
```python
.new_non_blocking_node(
   streams, functionObject, ip_to_num_channel = None, channel_to_ip = None,
   source_target_info = {}
)
```


### .new_blocking_node
[source](https://github.com/blob/master/quokka_runtime.py/#L380)
```python
.new_blocking_node(
   streams, functionObject, ip_to_num_channel = None, channel_to_ip = None,
   source_target_info = {}
)
```


### .create
[source](https://github.com/blob/master/quokka_runtime.py/#L402)
```python
.create()
```


### .run
[source](https://github.com/blob/master/quokka_runtime.py/#L412)
```python
.run()
```

