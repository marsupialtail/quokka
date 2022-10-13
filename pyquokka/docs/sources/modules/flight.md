#


## DiskFile
[source](https://github.com/blob/master/flight.py/#L13)
```python 
DiskFile(
   filename
)
```




**Methods:**


### .delete
[source](https://github.com/blob/master/flight.py/#L16)
```python
.delete()
```


----


## DiskQueue
[source](https://github.com/blob/master/flight.py/#L19)
```python 
DiskQueue(
   parents, prefix, disk_location
)
```




**Methods:**


### .append
[source](https://github.com/blob/master/flight.py/#L29)
```python
.append(
   key, batch, format
)
```


### .retrieve_from_disk
[source](https://github.com/blob/master/flight.py/#L41)
```python
.retrieve_from_disk(
   key
)
```


### .get_batches_for_key
[source](https://github.com/blob/master/flight.py/#L44)
```python
.get_batches_for_key(
   key, num = None
)
```


### .keys
[source](https://github.com/blob/master/flight.py/#L64)
```python
.keys()
```


### .len
[source](https://github.com/blob/master/flight.py/#L67)
```python
.len(
   key
)
```


### .get_all_len
[source](https://github.com/blob/master/flight.py/#L70)
```python
.get_all_len()
```


----


## FlightServer
[source](https://github.com/blob/master/flight.py/#L74)
```python 
FlightServer(
   host = 'localhost', location = None
)
```




**Methods:**


### .descriptor_to_key
[source](https://github.com/blob/master/flight.py/#L94)
```python
.descriptor_to_key(
   descriptor
)
```


### .list_flights
[source](https://github.com/blob/master/flight.py/#L108)
```python
.list_flights(
   context, criteria
)
```


### .get_flight_info
[source](https://github.com/blob/master/flight.py/#L117)
```python
.get_flight_info(
   context, descriptor
)
```


### .do_put
[source](https://github.com/blob/master/flight.py/#L131)
```python
.do_put(
   context, descriptor, reader, writer
)
```


### .number_batches
[source](https://github.com/blob/master/flight.py/#L172)
```python
.number_batches(
   batches
)
```


### .do_get
[source](https://github.com/blob/master/flight.py/#L176)
```python
.do_get(
   context, ticket
)
```


### .list_actions
[source](https://github.com/blob/master/flight.py/#L194)
```python
.list_actions(
   context
)
```


### .do_action
[source](https://github.com/blob/master/flight.py/#L202)
```python
.do_action(
   context, action
)
```

