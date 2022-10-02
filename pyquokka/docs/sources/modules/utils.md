#


## EC2Cluster
[source](https://github.com/blob/master/utils.py/#L10)
```python 
EC2Cluster(
   public_ips, private_ips, instance_ids, cpu_count_per_instance
)
```




**Methods:**


### .to_json
[source](https://github.com/blob/master/utils.py/#L32)
```python
.to_json(
   output = 'cluster.json'
)
```


----


## LocalCluster
[source](https://github.com/blob/master/utils.py/#L38)
```python 

```



----


## QuokkaClusterManager
[source](https://github.com/blob/master/utils.py/#L64)
```python 
QuokkaClusterManager(
   key_name = 'oregon-neurodb',
   key_location = '/home/ziheng/Downloads/oregon-neurodb.pem',
   security_group = 'sg-0770c1101ab26fba2'
)
```




**Methods:**


### .str_key_to_int
[source](https://github.com/blob/master/utils.py/#L72)
```python
.str_key_to_int(
   d
)
```


### .launch_all
[source](https://github.com/blob/master/utils.py/#L75)
```python
.launch_all(
   command, ips, error = 'Error'
)
```


### .copy_all
[source](https://github.com/blob/master/utils.py/#L82)
```python
.copy_all(
   file_path, ips, error = 'Error'
)
```


### .check_instance_alive
[source](https://github.com/blob/master/utils.py/#L89)
```python
.check_instance_alive(
   public_ip
)
```


### .create_cluster
[source](https://github.com/blob/master/utils.py/#L137)
```python
.create_cluster(
   aws_access_key, aws_access_id, num_instances = 1, instance_type = 'i3.2xlarge',
   requirements = ['ray =  = 1.12.0']
)
```


### .stop_cluster
[source](https://github.com/blob/master/utils.py/#L181)
```python
.stop_cluster(
   quokka_cluster
)
```


### .terminate_cluster
[source](https://github.com/blob/master/utils.py/#L196)
```python
.terminate_cluster(
   quokka_cluster
)
```


### .get_cluster_from_json
[source](https://github.com/blob/master/utils.py/#L211)
```python
.get_cluster_from_json(
   json_file
)
```

