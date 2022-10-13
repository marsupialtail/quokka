#


## RedisObjectsDataset
[source](https://github.com/blob/master/dataset.py/#L18)
```python 
RedisObjectsDataset(
   channel_objects, ip_set
)
```




**Methods:**


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L28)
```python
.get_next_batch(
   mapper_id, pos = None
)
```


----


## InputEC2ParquetDataset
[source](https://github.com/blob/master/dataset.py/#L48)
```python 
InputEC2ParquetDataset(
   bucket, prefix, columns = None, filters = None
)
```


---
The original plan was to split this up by row group and different channels might share a single file. This is too complicated and leads to high init cost.
Generally parquet files in a directory created by tools like Spark or Quokka have similar sizes anyway.


**Methods:**


### .get_own_state
[source](https://github.com/blob/master/dataset.py/#L65)
```python
.get_own_state(
   num_channels
)
```


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L83)
```python
.get_next_batch(
   mapper_id, pos = None
)
```


----


## InputParquetDataset
[source](https://github.com/blob/master/dataset.py/#L101)
```python 
InputParquetDataset(
   filepath, mode = 'local', columns = None, filters = None
)
```




**Methods:**


### .get_own_state
[source](https://github.com/blob/master/dataset.py/#L118)
```python
.get_own_state(
   num_channels
)
```


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L147)
```python
.get_next_batch(
   mapper_id, pos = None
)
```


----


## InputS3FilesDataset
[source](https://github.com/blob/master/dataset.py/#L172)
```python 
InputS3FilesDataset(
   bucket, prefix = None
)
```




**Methods:**


### .get_own_state
[source](https://github.com/blob/master/dataset.py/#L181)
```python
.get_own_state(
   num_channels
)
```


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L197)
```python
.get_next_batch(
   mapper_id, pos = None
)
```


----


## InputDiskFilesDataset
[source](https://github.com/blob/master/dataset.py/#L214)
```python 
InputDiskFilesDataset(
   directory
)
```




**Methods:**


### .get_own_state
[source](https://github.com/blob/master/dataset.py/#L223)
```python
.get_own_state(
   num_channels
)
```


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L227)
```python
.get_next_batch(
   mapper_id, pos = None
)
```


----


## InputDiskCSVDataset
[source](https://github.com/blob/master/dataset.py/#L239)
```python 
InputDiskCSVDataset(
   filepath, names = None, sep = ', ', stride = 16*1024*1024, header = False,
   window = 1024*4
)
```




**Methods:**


### .get_own_state
[source](https://github.com/blob/master/dataset.py/#L256)
```python
.get_own_state(
   num_channels
)
```


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L349)
```python
.get_next_batch(
   mapper_id, state = None
)
```


----


## InputS3CSVDataset
[source](https://github.com/blob/master/dataset.py/#L406)
```python 
InputS3CSVDataset(
   bucket, names = None, prefix = None, key = None, sep = ', ', stride = 64*1024*1024,
   header = False, window = 1024*32
)
```




**Methods:**


### .get_own_state
[source](https://github.com/blob/master/dataset.py/#L427)
```python
.get_own_state(
   num_channels
)
```


### .get_next_batch
[source](https://github.com/blob/master/dataset.py/#L530)
```python
.get_next_batch(
   mapper_id, state = None
)
```

