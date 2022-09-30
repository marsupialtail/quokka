#


## SortPhase2Dataset
[source](https://github.com/blob/master/dataset.py/#L23)
```python 
SortPhase2Dataset(
   channel_files, key, record_batch_rows
)
```




**Methods:**


### .set_num_channels
[source](https://github.com/blob/master/dataset.py/#L30)
```python
.set_num_channels(
   num_channels
)
```


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L33)
```python
.get_next_batch(
   mapper_id, pos = None
)
```


----


## RedisObjectsDataset
[source](https://github.com/blob/master/dataset.py/#L78)
```python 
RedisObjectsDataset(
   channel_objects, ip_set
)
```




**Methods:**


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L88)
```python
.get_next_batch(
   mapper_id, pos = None
)
```


----


## InputDiskHDF5Dataset
[source](https://github.com/blob/master/dataset.py/#L105)
```python 
InputDiskHDF5Dataset(
   filename, key
)
```




**Methods:**


### .set_num_channels
[source](https://github.com/blob/master/dataset.py/#L114)
```python
.set_num_channels(
   num_channels
)
```


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L124)
```python
.get_next_batch(
   mapper_id, pos = None
)
```


----


## InputEC2ParquetDataset
[source](https://github.com/blob/master/dataset.py/#L139)
```python 
InputEC2ParquetDataset(
   filepath, mode = 'local', columns = None, filters = None
)
```




**Methods:**


### .set_num_channels
[source](https://github.com/blob/master/dataset.py/#L156)
```python
.set_num_channels(
   num_channels
)
```


### .get_own_state
[source](https://github.com/blob/master/dataset.py/#L159)
```python
.get_own_state(
   num_channels
)
```


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L191)
```python
.get_next_batch(
   mapper_id, pos = None
)
```


----


## InputParquetDataset
[source](https://github.com/blob/master/dataset.py/#L211)
```python 
InputParquetDataset(
   filepath, mode = 'local', columns = None, filters = None
)
```




**Methods:**


### .set_num_channels
[source](https://github.com/blob/master/dataset.py/#L228)
```python
.set_num_channels(
   num_channels
)
```


### .get_own_state
[source](https://github.com/blob/master/dataset.py/#L231)
```python
.get_own_state(
   num_channels
)
```


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L260)
```python
.get_next_batch(
   mapper_id, pos = None
)
```


----


## InputS3FilesDataset
[source](https://github.com/blob/master/dataset.py/#L273)
```python 
InputS3FilesDataset(
   bucket, prefix = None
)
```




**Methods:**


### .set_num_channels
[source](https://github.com/blob/master/dataset.py/#L282)
```python
.set_num_channels(
   num_channels
)
```


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L298)
```python
.get_next_batch(
   mapper_id, pos = None
)
```


----


## InputDiskFilesDataset
[source](https://github.com/blob/master/dataset.py/#L314)
```python 
InputDiskFilesDataset(
   directory
)
```




**Methods:**


### .set_num_channels
[source](https://github.com/blob/master/dataset.py/#L323)
```python
.set_num_channels(
   num_channels
)
```


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L327)
```python
.get_next_batch(
   mapper_id, pos = None
)
```


----


## InputDiskCSVDataset
[source](https://github.com/blob/master/dataset.py/#L339)
```python 
InputDiskCSVDataset(
   filepath, names = None, sep = ', ', stride = 16*1024*1024, header = False,
   window = 1024*4
)
```




**Methods:**


### .set_num_channels
[source](https://github.com/blob/master/dataset.py/#L355)
```python
.set_num_channels(
   num_channels
)
```


### .get_own_state
[source](https://github.com/blob/master/dataset.py/#L359)
```python
.get_own_state(
   num_channels
)
```


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L452)
```python
.get_next_batch(
   mapper_id, state = None
)
```


----


## InputS3CSVDataset
[source](https://github.com/blob/master/dataset.py/#L505)
```python 
InputS3CSVDataset(
   bucket, names = None, prefix = None, key = None, sep = ', ', stride = 64*1024*1024,
   header = False, window = 1024*32
)
```




**Methods:**


### .set_num_channels
[source](https://github.com/blob/master/dataset.py/#L525)
```python
.set_num_channels(
   num_channels
)
```


### .get_own_state
[source](https://github.com/blob/master/dataset.py/#L530)
```python
.get_own_state(
   num_channels
)
```


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L633)
```python
.get_next_batch(
   mapper_id, state = None
)
```

