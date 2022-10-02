#


## RedisObjectsDataset
[source](https://github.com/blob/master/dataset.py/#L24)
```python 
RedisObjectsDataset(
   channel_objects, ip_set
)
```




**Methods:**


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L34)
```python
.get_next_batch(
   mapper_id, pos = None
)
```


----


## InputEC2ParquetDataset
[source](https://github.com/blob/master/dataset.py/#L54)
```python 
InputEC2ParquetDataset(
   filepath, columns = None, filters = None
)
```


---
The original plan was to split this up by row group and different channels might share a single file. This is too complicated and leads to high init cost.
Generally parquet files in a directory created by tools like Spark or Quokka have similar sizes anyway.


**Methods:**


### .get_own_state
[source](https://github.com/blob/master/dataset.py/#L78)
```python
.get_own_state(
   num_channels
)
```


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L96)
```python
.get_next_batch(
   mapper_id, pos = None
)
```


----


## InputParquetDataset
[source](https://github.com/blob/master/dataset.py/#L122)
```python 
InputParquetDataset(
   filepath, mode = 'local', columns = None, filters = None
)
```




**Methods:**


### .get_own_state
[source](https://github.com/blob/master/dataset.py/#L139)
```python
.get_own_state(
   num_channels
)
```


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L168)
```python
.get_next_batch(
   mapper_id, pos = None
)
```


----


## InputS3FilesDataset
[source](https://github.com/blob/master/dataset.py/#L181)
```python 
InputS3FilesDataset(
   bucket, prefix = None
)
```




**Methods:**


### .get_own_state
[source](https://github.com/blob/master/dataset.py/#L190)
```python
.get_own_state(
   num_channels
)
```


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L206)
```python
.get_next_batch(
   mapper_id, pos = None
)
```


----


## InputDiskFilesDataset
[source](https://github.com/blob/master/dataset.py/#L223)
```python 
InputDiskFilesDataset(
   directory
)
```




**Methods:**


### .get_own_state
[source](https://github.com/blob/master/dataset.py/#L232)
```python
.get_own_state(
   num_channels
)
```


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L236)
```python
.get_next_batch(
   mapper_id, pos = None
)
```


----


## InputDiskCSVDataset
[source](https://github.com/blob/master/dataset.py/#L248)
```python 
InputDiskCSVDataset(
   filepath, names = None, sep = ', ', stride = 16*1024*1024, header = False,
   window = 1024*4
)
```




**Methods:**


### .get_own_state
[source](https://github.com/blob/master/dataset.py/#L265)
```python
.get_own_state(
   num_channels
)
```


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L358)
```python
.get_next_batch(
   mapper_id, state = None
)
```


----


## InputS3CSVDataset
[source](https://github.com/blob/master/dataset.py/#L411)
```python 
InputS3CSVDataset(
   bucket, names = None, prefix = None, key = None, sep = ', ', stride = 64*1024*1024,
   header = False, window = 1024*32
)
```




**Methods:**


### .get_own_state
[source](https://github.com/blob/master/dataset.py/#L432)
```python
.get_own_state(
   num_channels
)
```


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L535)
```python
.get_next_batch(
   mapper_id, state = None
)
```

