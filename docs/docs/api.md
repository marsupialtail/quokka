# API reference

First do: 
~~~python
from pyquokka.df import * 
qc = QuokkaContext()
~~~

If working with S3, do:
~~~python
from pyquokka.df import * 
manager = QuokkaClusterManager()
cluster = manager.get_cluster_from_json("config.json")
~~~
This assumes you have a cluster saved in config.json. Please refer to the guide [here](cloud.md) to do this.

## qc.read_csv