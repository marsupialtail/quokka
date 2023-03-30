import copy
import polars
import pyquokka.sql_utils as sql_utils
from pyquokka.datastream import * 
from pyquokka.catalog import *
import pyarrow.parquet as pq
from pyarrow.fs import S3FileSystem
import os

class QuokkaContext:
    def __init__(self, cluster = None, io_per_node = 2, exec_per_node = 1) -> None:

        """
        Creates a QuokkaContext object. Needed to do basically anything. Similar to SparkContext.

        Args:
            cluster (Cluster): Cluster object. If None, a LocalCluster will be created.
            io_per_node (int): Number of IO nodes to launch per machine. Default is 2. This controls the number of IO thread pools to use per machine.
            exec_per_node (int): Number of compute nodes to launch per machine. Default is 1. This controls the number of compute thread pools to use per machine.
        
        Returns:
            QuokkaContext object
        
        Examples:

            >>> from pyquokka.df import QuokkaContext

            This will create a QuokkaContext object with a LocalCluster to execute locally. Useful for testing and exploration.

            >>> qc = QuokkaContext()

            If you want to connect to a remote Ray cluster, you can do this.

            >>> manager = QuokkaClusterManager()
            >>> cluster = manager.get_cluster_from_ray("my_cluster.yaml", aws_access_key, aws_access_id, requirements = ["numpy", "pandas"], spill_dir = "/data")
            >>> from pyquokka.df import QuokkaContext
            >>> qc = QuokkaContext(cluster)

            Or you can spin up a new cluster:

            >>> manager =  QuokkaClusterManager(key_name = "my_key", key_location = "/home/ubuntu/.ssh/my_key.pem", security_group = "my_security_group")
            >>> cluster = manager.create_cluster(aws_access_key, aws_access_id, num_instances = 2, instance_type = "i3.2xlarge", ami="ami-0530ca8899fac469f", requirements = ["numpy", "pandas"])
            >>> qc = QuokkaContext(cluster)

            Or you can connect to a stopped cluster that was saved to json.

            >>> from pyquokka.utils import *
            >>> manager =  QuokkaClusterManager(key_name = "my_key", key_location = "/home/ubuntu/.ssh/my_key.pem", security_group = "my_security_group")
            >>> cluster = manager.create_cluster(aws_access_key, aws_access_id, num_instances = 2, instance_type = "i3.2xlarge", ami="ami-0530ca8899fac469f", requirements = ["numpy", "pandas"])
            >>> cluster.to_json("my_cluster.json")

            You can now close this Python session. In a new Python session you can connect to this cluster by doing:

            >>> from pyquokka.utils import *
            >>> manager = QuokkaClusterManager(key_name = "my_key", key_location = "/home/ubuntu/.ssh/my_key.pem", security_group = "my_security_group")
            >>> cluster = manager.from_json("my_cluster.json")

        """

        self.sql_config= {"optimize_joins" : True, "s3_csv_materialize_threshold" : 10 * 1048576, "disk_csv_materialize_threshold" : 1048576,
                      "s3_parquet_materialize_threshold" : 10 * 1048576, "disk_parquet_materialize_threshold" : 1048576}
        self.exec_config = {"hbq_path": "/data/", "fault_tolerance": False, "memory_limit": 0.25, "max_pipeline_batches": 30, 
                        "checkpoint_interval": None, "checkpoint_bucket": "quokka-checkpoint", "batch_attempt": 5}

        self.latest_node_id = 0
        self.nodes = {}
        self.cluster = LocalCluster() if cluster is None else cluster
        if type(self.cluster) == LocalCluster:
            self.exec_config["fault_tolerance"] = False, "Fault tolerance is not supported in local mode, turning it off"
        self.io_per_node = io_per_node
        self.exec_per_node = exec_per_node

        self.coordinator = Coordinator.options(num_cpus=0.001, max_concurrency = 2,resources={"node:" + str(self.cluster.leader_private_ip): 0.001}).remote()
        self.catalog = Catalog.options(num_cpus=0.001,resources={"node:" + str(self.cluster.leader_private_ip): 0.001}).remote()
        self.dataset_manager = ArrowDataset.options(num_cpus = 0.001, resources={"node:" + str(self.cluster.leader_private_ip): 0.001}).remote()

        self.task_managers = {}
        self.node_locs= {}
        self.io_nodes = set()
        self.compute_nodes = set()
        self.replay_nodes = set()
        count = 0

        self.leader_compute_nodes = []
        self.leader_io_nodes = []

        # default strategy launches two IO nodes and one compute node per machine
        private_ips = list(self.cluster.private_ips.values())
        for ip in private_ips:
            
            for k in range(1):
                self.task_managers[count] = ReplayTaskManager.options(num_cpus = 0.001, max_concurrency = 2, resources={"node:" + ip : 0.001}).remote(count, self.cluster.leader_private_ip, list(self.cluster.private_ips.values()), self.exec_config)
                self.replay_nodes.add(count)
                self.node_locs[count] = ip
                count += 1
            for k in range(io_per_node):
                self.task_managers[count] = IOTaskManager.options(num_cpus = 0.001, max_concurrency = 2, resources={"node:" + ip : 0.001}).remote(count, self.cluster.leader_private_ip, list(self.cluster.private_ips.values()), self.exec_config)
                self.io_nodes.add(count)
                self.node_locs[count] = ip
                count += 1

                if ip == self.cluster.leader_private_ip:
                    self.leader_io_nodes.append(count)

            for k in range(exec_per_node):
                if type(self.cluster) == LocalCluster:
                    self.task_managers[count] = ExecTaskManager.options(num_cpus = 0.001, max_concurrency = 2, resources={"node:" + ip : 0.001}).remote(count, self.cluster.leader_private_ip, list(self.cluster.private_ips.values()), self.exec_config)
                elif type(self.cluster) == EC2Cluster:
                    self.task_managers[count] = ExecTaskManager.options(num_cpus = 0.001, max_concurrency = 2, resources={"node:" + ip : 0.001}).remote(count, self.cluster.leader_private_ip, list(self.cluster.private_ips.values()), self.exec_config) 
                else:
                    raise Exception

                if ip == self.cluster.leader_private_ip:
                    self.leader_compute_nodes.append(count)
                
                self.compute_nodes.add(count)
                self.node_locs[count] = ip
                count += 1        

        ray.get(self.coordinator.register_nodes.remote(replay_nodes = {k: self.task_managers[k] for k in self.replay_nodes}, io_nodes = {k: self.task_managers[k] for k in self.io_nodes}, compute_nodes = {k: self.task_managers[k] for k in self.compute_nodes}))
        ray.get(self.coordinator.register_node_ips.remote( self.node_locs ))

    def set_config(self, key, value):

        """
        This sets a config value for the entire cluster. You should do this at the very start of your program generally speaking.

        The following keys are supported:

        1. optimize_joins: bool, whether to optimize joins based on cardinality estimates. Default to True

        2. s3_csv_materialize_threshold: int, the threshold in bytes for when to materialize a CSV file in S3

        3. disk_csv_materialize_threshold: int, the threshold in bytes for when to materialize a CSV file on disk

        4. s3_parquet_materialize_threshold: int, the threshold in bytes for when to materialize a Parquet file in S3

        5. disk_parquet_materialize_threshold: int, the threshold in bytes for when to materialize a Parquet file on disk

        6. hbq_path: str, the disk spill directory. Default to "/data"

        7. fault_tolerance: bool, whether to enable fault tolerance. Default to False

        Args:
            key (str): the key to set
            value (any): the value to set

        Returns:
            None
        
        Examples:

            >>> from pyquokka.df import *
            >>> qc = QuokkaContext()

            Turn on join order optimization.

            >>> qc.set_config("optimize_joins", True)

            Turn off fault tolerance. 

            >>> qc.set_config("fault_tolerance", False)
        
        """

        if key in self.sql_config:
            self.sql_config[key] = value
        elif key in self.exec_config:
            self.exec_config[key] = value
            assert all(ray.get([task_manager.set_config.remote(key, value) for task_manager in self.task_managers.values()]))
        else:
            raise Exception("key not found in config")
    
    def get_config(self, key):

        """
        Gets a config value for the entire cluster.

        Args:
            key (str): the key to get

        Returns:
            any: the value of the key

        Examples:

            >>> from pyquokka.df import *
            >>> qc = QuokkaContext()
            >>> qc.get_config("optimize_joins")
        
        """

        if key in self.sql_config:
            return self.sql_config[key]
        elif key in self.exec_config:
            return self.exec_config[key]
        else:
            raise Exception("key not found in config")

    def read_files(self, table_location: str):

        """
        This doesn't work yet due to difficulty handling Object types in Polars
        """

        if table_location[:5] == "s3://":

            if type(self.cluster) == LocalCluster:
                print("Warning: trying to read S3 dataset on local machine. This assumes high network bandwidth.")

            table_location = table_location[5:]
            bucket = table_location.split("/")[0]
            if "*" in table_location:
                assert table_location[-1] == "*" , "wildcard can only be the last character in address string"
                prefix = "/".join(table_location[:-1].split("/")[1:])
                self.nodes[self.latest_node_id] = InputS3FilesNode(bucket, prefix,["filename","object"])
            else:
                raise NotImplemented("Are you trying to read a single file? It's not supported. Please use s3://bucket/prefix*. Add the asterisk!")
        else:
            if type(self.cluster) == EC2Cluster:
                raise NotImplementedError("Does not support reading local dataset with S3 cluster. Must use S3 bucket.")

            if "*" in table_location:
                table_location = table_location[:-1]
                assert "*" not in table_location, "* only supported at the end"
                assert table_location[-1] == "/", "must specify * with entire directory, doesn't support prefixes yet"
                assert os.path.isdir(table_location), "must supply absolute path"

                self.nodes[self.latest_node_id] = InputDiskFilesNode(table_location, ["filename","object"])
            elif table_location[-1] == "/":
                assert os.path.isdir(table_location), "must supply absolute path"
                self.nodes[self.latest_node_id] = InputDiskFilesNode(table_location, ["filename","object"])
            else:
                raise NotImplemented("Are you trying to read a single file? It's not supported. Please supply absolute directory to the files, like /tmp/*. Add the asterisk!")
            
            # if local, you should not launch too many actors since not that many needed to saturate disk
            # self.nodes[self.latest_node_id].set_placement_strategy(CustomChannelsStrategy(2))

        self.latest_node_id += 1
        return DataStream(self, ["filename","object"], self.latest_node_id - 1)

    '''
    The API layer for read_csv mainly do four things in sequence:
    - Detect if it's a S3 location or a disk location
    - Detect if it's a list of files or a single file
    - Detect if the data is small enough (< 10MB and single file) to be immediately materialized into a Polars dataframe.
    - Detect the schema if not supplied. This is not needed by the reader but is needed by the logical plan optimizer.
    After it has done these things, if the dataset is not materialized, we will instantiate a logical plan node and return a DataStream
    '''

    def read_csv(self, table_location: str, schema = None, has_header = False, sep=","):

        """
        Read in a CSV file or files from a table location. It can be a single CSV or a list of CSVs. It can be CSV(s) on disk
        or CSV(s) on S3. Currently other clouds are not supported. The CSVs can have a predefined schema using a list of 
        column names in the schema argument, or you can specify the CSV has a header row and Quokka will read the schema 
        from it. You should also specify the CSV's separator. 

        Args:
            table_location (str): where the CSV(s) are. This mostly mimics Spark behavior. Look at the examples.
            schema (list): you can provide a list of column names, it's kinda like polars.read_csv(new_columns=...)
            has_header (bool): is there a header row. If the schema is not provided, this should be True. If the schema IS provided, 
                this can still be True. Quokka will just ignore the header row.
            sep (str): default to ',' but could be something else, like '|' for TPC-H tables

        Return:
            A DataStream.
        
        Examples:

            Read a single CSV. It's better always to specify the absolute path.
            >>> lineitem = qc.read_csv("/home/ubuntu/tpch/lineitem.csv")

            Read a directory of CSVs 
            >>> lineitem = qc.read_csv("/home/ubuntu/tpch/lineitem/*")

            Read a single CSV from S3
            >>> lineitem = qc.read_csv("s3://tpc-h-csv/lineitem/lineitem.tbl.1")

            Read CSVs from S3 bucket with prefix
            >>> lineitem = qc.read_csv("s3://tpc-h-csv/lineitem/*")
            ~~~
        """

        def get_schema_from_bytes(resp):
            first_newline = resp.find(bytes('\n', 'utf-8'))
            if first_newline == -1:
                raise Exception("could not detect the first line break with first 4 kb")
            resp = resp[:first_newline]
            if resp[-1] == sep:
                resp = resp[:-1]
            schema = resp.decode("utf-8").split(sep)
            return schema
    
        def return_materialized_stream(where, my_schema = None):
            if has_header:
                df = polars.read_csv(where, has_header = True,sep = sep)
            else:
                df = polars.read_csv(where, new_columns = my_schema, has_header = False,sep = sep)
            self.nodes[self.latest_node_id] = InputPolarsNode(df)
            self.latest_node_id += 1
            return DataStream(self, df.columns, self.latest_node_id - 1, materialized=True)

        if schema is None:
            assert has_header, "if not provide schema, must have header."
        if schema is not None and has_header:
            print("You specified a schema as well as a header. Quokka should use your schema and ignore the names in the header.")

        if table_location[:5] == "s3://":

            if type(self.cluster) == LocalCluster:
                print("Warning: trying to read S3 dataset on local machine. This assumes high network bandwidth.")

            table_location = table_location[5:]
            bucket = table_location.split("/")[0]
            if "*" in table_location:
                assert table_location[-1] == "*" , "wildcard can only be the last character in address string"
                prefix = "/".join(table_location[:-1].split("/")[1:])

                s3 = boto3.client('s3')
                z = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
                if 'Contents' not in z:
                    raise Exception("Wrong S3 path")
                files = [i['Key'] for i in z['Contents']]
                sizes = [i['Size'] for i in z['Contents']]
                assert 'NextContinuationToken' not in z, "too many files in S3 bucket"
                assert len(files) > 0, "no files under prefix"

                if schema is None:
                    resp = s3.get_object(
                        Bucket=bucket, Key=files[0], Range='bytes={}-{}'.format(0, 4096))['Body'].read()
                    schema = get_schema_from_bytes(resp)

                # if there are more than one files, there could be header problems. just do one file right now
                if len(files) == 1 and sizes[0] < self.sql_config["s3_csv_materialize_threshold"]:
                    return return_materialized_stream("s3://" + bucket + "/" + files[0], schema)

                token = ray.get(self.catalog.register_s3_csv_source.remote(bucket, files[0], schema, sep, sum(sizes)))
                self.nodes[self.latest_node_id] = InputS3CSVNode(bucket, prefix, None, schema, sep, has_header)
                self.nodes[self.latest_node_id].set_catalog_id(token)
                
            else:
                key = "/".join(table_location.split("/")[1:])
                s3 = boto3.client('s3')
                try:
                    response = s3.head_object(Bucket= bucket, Key=key)
                    size = response['ContentLength']
                except:
                    raise Exception("CSV not found in S3!")
                if schema is None:
                    resp = s3.get_object(
                        Bucket=bucket, Key=key, Range='bytes={}-{}'.format(0, 4096))['Body'].read()
                    schema = get_schema_from_bytes(resp)

                if size < 10 * 1048576:
                    return return_materialized_stream("s3://" + table_location, schema)
                
                token = ray.get(self.catalog.register_s3_csv_source.remote(bucket, key, schema, sep, size))
                self.nodes[self.latest_node_id] = InputS3CSVNode(bucket, None, key, schema, sep, has_header)
                self.nodes[self.latest_node_id].set_catalog_id(token)
        else:

            if type(self.cluster) == EC2Cluster:
                raise NotImplementedError("Does not support reading local dataset with S3 cluster. Must use S3 bucket.")

            if "*" in table_location:
                table_location = table_location[:-1]
                assert table_location[-1] == "/", "must specify * with entire directory, doesn't support prefixes yet"
                try:
                    files = [i for i in os.listdir(table_location)]
                except:
                    raise Exception("Tried to get list of files at ", table_location, " failed. Make sure specify absolute path")
                assert len(files) > 0

                if schema is None:
                    resp = open(table_location + files[0],"rb").read(1024 * 4)
                    schema = get_schema_from_bytes(resp)

                if len(files) == 1 and os.path.getsize(table_location + files[0]) < self.sql_config["disk_csv_materialize_threshold"]:
                    return return_materialized_stream(table_location + files[0], schema)

                token = ray.get(self.catalog.register_disk_csv_source.remote(table_location, schema, sep))
                self.nodes[self.latest_node_id] = InputDiskCSVNode(table_location, schema, sep, has_header)
                self.nodes[self.latest_node_id].set_catalog_id(token)
            else:
                size = os.path.getsize(table_location)
                if schema is None:
                    resp = open(table_location,"rb").read(1024 * 4)
                    schema = get_schema_from_bytes(resp)
                if size < self.sql_config["disk_csv_materialize_threshold"]:
                    return return_materialized_stream(table_location, schema)
                
                token = ray.get(self.catalog.register_disk_csv_source.remote(table_location, schema, sep))
                self.nodes[self.latest_node_id] = InputDiskCSVNode(table_location, schema, sep, has_header)
                self.nodes[self.latest_node_id].set_catalog_id(token)
            
        self.latest_node_id += 1
        return DataStream(self, schema, self.latest_node_id - 1)

    def read_parquet(self, table_location: str):

        """
        Read Parquet. It can be a single Parquet or a list of Parquets. It can be Parquet(s) on disk
        or Parquet(s) on S3. Currently other clouds are not supported. 

        Args:
            table_location (str): where the Parquet(s) are. This mostly mimics Spark behavior. Look at the examples.

        Return:
            DataStream.
        
        Examples:
            
            Read a single Parquet. It's better always to specify the absolute path.
            >>> lineitem = qc.read_parquet("/home/ubuntu/tpch/lineitem.parquet")

            Read a directory of Parquets 
            >>> lineitem = qc.read_parquet("/home/ubuntu/tpch/lineitem/*")

            Read a single Parquet from S3
            >>> lineitem = qc.read_parquet("s3://tpc-h-parquet/lineitem.parquet")

            Read Parquets from S3 bucket with prefix
            >>> lineitem = qc.read_parquet("s3://tpc-h-parquet/lineitem.parquet/*")
        """

        def return_materialized_stream(df):
            self.nodes[self.latest_node_id] = InputPolarsNode(df)
            self.latest_node_id += 1
            return DataStream(self, df.columns, self.latest_node_id - 1, materialized=True)

        s3 = boto3.client('s3')
        if table_location[:5] == "s3://":

            if type(self.cluster) == LocalCluster:
                print("Warning: trying to read S3 dataset on local machine. This assumes high network bandwidth.")

            table_location = table_location[5:]
            bucket = table_location.split("/")[0]
            if "*" in table_location:
                assert "*" not in table_location[:-1], "wildcard can only be the last character in address string"
                table_location = table_location[:-1]
                prefix = "/".join(table_location[:-1].split("/")[1:])

                z = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
                if 'Contents' not in z:
                    raise Exception("Wrong S3 path")
                files = [bucket + "/" + i['Key'] for i in z['Contents'] if i['Key'].endswith(".parquet")]
                sizes = [i['Size'] for i in z['Contents'] if i['Key'].endswith('.parquet')]
                while 'NextContinuationToken' in z.keys():
                    z = s3.list_objects_v2(
                        Bucket=bucket, Prefix=prefix, ContinuationToken=z['NextContinuationToken'])
                    files.extend([bucket + "/" + i['Key'] for i in z['Contents']
                                    if i['Key'].endswith(".parquet")])
                    sizes.extend([i['Size'] for i in z['Contents'] if i['Key'].endswith('.parquet')])

                assert len(files) > 0, "could not find any parquet files. make sure they end with .parquet"
                if sum(sizes) < self.sql_config["s3_parquet_materialize_threshold"] and len(files) == 1:
                    df = polars.from_arrow(pq.read_table(files[0], filesystem = S3FileSystem()))
                    return return_materialized_stream(df)
                
                try:
                    f = pq.ParquetFile(S3FileSystem().open_input_file(files[0]))
                    schema = [k.name for k in f.schema_arrow]
                except:
                    raise Exception("schema discovery failed for Parquet dataset at location {}. Please raise Github issue.".format(table_location))
                
                token = ray.get(self.catalog.register_s3_parquet_source.remote(files[0], len(sizes)))
                self.nodes[self.latest_node_id] = InputS3ParquetNode(files, schema)
                self.nodes[self.latest_node_id].set_catalog_id(token)
            else:
                try:
                    f = pq.ParquetFile(S3FileSystem().open_input_file(table_location))
                    schema = [k.name for k in f.schema_arrow]
                except:
                    raise Exception("""schema discovery failed for Parquet dataset at location {}. 
                                    Note if you are specifying a prefix to many parquet files, must use asterix. E.g.
                                    qc.read_parquet("s3://rottnest/happy.parquet/*")""".format(table_location))
                key = "/".join(table_location.split("/")[1:])
                response = s3.head_object(Bucket= bucket, Key=key)
                size = response['ContentLength']
                if size < self.sql_config["s3_parquet_materialize_threshold"]:
                    df = polars.from_arrow(pq.read_table(table_location, filesystem = S3FileSystem()))
                    return return_materialized_stream(df)
                
                token = ray.get(self.catalog.register_s3_parquet_source.remote(bucket + "/" + key, 1))
                self.nodes[self.latest_node_id] = InputS3ParquetNode([table_location], schema)
                self.nodes[self.latest_node_id].set_catalog_id(token)

            # self.nodes[self.latest_node_id].set_placement_strategy(CustomChannelsStrategy(2))
        else:
            if type(self.cluster) == EC2Cluster:
                raise NotImplementedError("Does not support reading local dataset with S3 cluster. Must use S3 bucket.")
            
            if "*" in table_location:
                table_location = table_location[:-1]
                assert table_location[-1] == "/", "must specify * with entire directory, doesn't support prefixes yet"
                try:
                    files = [i for i in os.listdir(table_location) if i.endswith(".parquet")]
                except:
                    raise Exception("Tried to get list of parquet files at ", table_location, " failed. Make sure specify absolute path and filenames end with .parquet")
                assert len(files) > 0
                f = pq.ParquetFile(table_location + files[0])
                schema = [k.name for k in f.schema_arrow]
                if len(files) == 1 and os.path.getsize(table_location + files[0]) < self.sql_config["disk_parquet_materialize_threshold"]:
                    df = polars.read_parquet(table_location + files[0])
                    return return_materialized_stream(df)
                
                token = ray.get(self.catalog.register_disk_parquet_source.remote(table_location))
                self.nodes[self.latest_node_id] = InputDiskParquetNode(table_location, schema)
                self.nodes[self.latest_node_id].set_catalog_id(token)

            else:
                try:
                    size = os.path.getsize(table_location)
                except:
                    raise Exception("could not find the parquet file at ", table_location)
                
                if size < self.sql_config["disk_parquet_materialize_threshold"]:
                    df = polars.read_parquet(table_location)
                    return return_materialized_stream(df)
                
                f = pq.ParquetFile(table_location)
                schema = [k.name for k in f.schema_arrow]
                token = ray.get(self.catalog.register_disk_parquet_source.remote(table_location))
                self.nodes[self.latest_node_id] = InputDiskParquetNode(table_location, schema)
                self.nodes[self.latest_node_id].set_catalog_id(token)

        self.latest_node_id += 1
        return DataStream(self, schema, self.latest_node_id - 1)
    

    def read_rest_post(self, url, data, schema, headers = {}, batch_size = 10000):
        self.nodes[self.latest_node_id] = InputRestPostAPINode(url, data, headers, schema, batch_size = batch_size)
        self.latest_node_id += 1
        return DataStream(self, schema, self.latest_node_id - 1)
    
    def read_rest_get(self, url, data, schema, headers = {}, batch_size = 10000):
        self.nodes[self.latest_node_id] = InputRestGetAPINode(url, data, headers, schema, batch_size = batch_size)
        self.latest_node_id += 1
        return DataStream(self, schema, self.latest_node_id - 1)

    def read_dataset(self, dataset):

        """
        Convert a DataSet back to a DataStream to process it.

        Args:
            dataset (DataSet): The dataset to read from. Note this is a Quokka DataSet, not a Ray Data dataset!

        Returns:
            DataStream: The data stream.

        Examples:

            >>> ds = qc.read_parquet("s3://my-bucket/my-data.parquet").compute()

            `ds` will be a DataSet, i.e. a collection of Ray ObjectRefs. 

            >>> ds = qc.read_dataset(ds)

            `ds` will now be a DataStream.
        """

        self.nodes[self.latest_node_id] = InputRayDatasetNode(dataset)
        self.latest_node_id += 1
        return DataStream(self, dataset.schema, self.latest_node_id - 1)

    def read_ray_dataset(self, dataset, get_size = True):
        dataset_id = ray.get(self.dataset_manager.create_dataset.remote())
        my_dataset = Dataset(dataset.schema().names, self.dataset_manager, dataset_id)
        arrow_refs = dataset.to_arrow_refs()
        if get_size:
            size = dataset.count() // len(arrow_refs)
        else:
            size = None
        handles = []
        nodes = ray.nodes()
        for arrow_ref in arrow_refs:
            my_node = ray.experimental.get_object_locations([arrow_ref])[arrow_ref]['node_ids']
            assert len(my_node) == 1, "arrow ref should only be on one node, please raise Github issue."
            my_node = my_node[0]
            node_ip = [i['NodeManagerAddress'] for i in nodes if i['NodeID'] == my_node]
            assert len(node_ip) == 1, "node id should only be on one IP, please raise Github issue."
            node_ip = node_ip[0]
            handles.append(self.dataset_manager.added_object.remote(dataset_id, node_ip, [arrow_ref, size]))
        ray.get(handles)
        return self.read_dataset(my_dataset)


    def from_polars(self, df):

        """
        Create a DataStream from a polars DataFrame. The DataFrame will be materialized. If you don't know what this means, don't worry about it.

        Args:
            df (Polars DataFrame): The polars DataFrame to create the DataStream from.
        
        Returns:
            DataStream: The DataStream created from the polars DataFrame.
        
        Examples:

            >>> import polars as pl
            >>> from pyquokka.df import QuokkaContext
            >>> qc = QuokkaContext()
            >>> df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
            >>> stream = qc.from_polars(df)
            >>> stream.count()
        """

        self.nodes[self.latest_node_id] = InputPolarsNode(df)
        self.latest_node_id += 1
        return DataStream(self, df.columns, self.latest_node_id - 1, materialized=True)

    def from_pandas(self, df):

        """
        Create a DataStream from a pandas DataFrame. The DataFrame will be materialized. If you don't know what this means, don't worry about it.

        Args:
            df (Pandas DataFrame): The pandas DataFrame to create the DataStream from.

        Returns:
            DataStream: The DataStream created from the pandas DataFrame.

        Examples:

            >>> import pandas as pd
            >>> from pyquokka.df import QuokkaContext
            >>> qc = QuokkaContext()
            >>> df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
            >>> stream = qc.from_pandas(df)
            >>> stream.count()

        """

        self.nodes[self.latest_node_id] = InputPolarsNode(polars.from_pandas(df))
        self.latest_node_id += 1
        return DataStream(self, df.columns, self.latest_node_id - 1, materialized=True)

    def from_arrow(self, df):

        """
        Create a DataStream for a pyarrow Table. The DataFrame will be materialized. If you don't know what this means, don't worry about it.

        Args:
            df (PyArrow Table): The polars DataFrame to create the DataStream from.
        
        Returns:
            DataStream: The DataStream created from the polars DataFrame.
        
        Examples:

            >>> import polars as pl
            >>> from pyquokka.df import QuokkaContext
            >>> qc = QuokkaContext()
            >>> df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).to_arrow()
            >>> stream = qc.from_arrow(df)
            >>> stream.count()
        
        """

        self.nodes[self.latest_node_id] = InputPolarsNode(polars.from_arrow(df))
        self.latest_node_id += 1
        return DataStream(self, df.columns, self.latest_node_id - 1, materialized=True)

    def read_sorted_parquet(self, table_location: str, sorted_by: str, schema = None):
        assert type(sorted_by) == str
        stream = self.read_parquet(table_location, schema)
        stream._set_sorted({sorted_by : "stride"})
        return stream
    
    def read_sorted_csv(self, table_location: str,sorted_by: str, schema = None, has_header = False, sep=","):
        assert type(sorted_by) == str
        stream = self.read_csv(table_location, schema, has_header, sep)
        stream._set_sorted({sorted_by : "stride"})
        return stream

    def read_iceberg(self, table, snapshot = None):

        """
        Must have pyiceberg installed on the client. This is a new API. This only supports AWS Glue catalog so far. 

        Args:
            table (str): The table name to read from. This is the full table name, not just the table name. For example, if the table is in the database "default" and the table name is "my_table", then the table name should be "default.my_table".
            snapshot (int): The snapshot ID to read from. If this is not specified, the latest snapshot will be read from.
        
        Returns:
            DataStream: The DataStream created from the iceberg table.
        
        Examples:

            >>> from pyquokka.df import QuokkaContext
            >>> qc = QuokkaContext()
            >>> stream = qc.read_iceberg("default.my_table")
        """

        from pyiceberg.catalog import load_catalog
        catalog = load_catalog("glue", **{"type":"glue"})
        try:
            table = catalog.load_table(table)
        except:
            raise Exception("table not found")
        if snapshot is not None:
            assert snapshot in [i.snapshot_id for i in table.metadata.snapshots], "snapshot not found"

        self.nodes[self.latest_node_id] = InputS3IcebergNode(table, snapshot)
        self.latest_node_id += 1
        return DataStream(self, [i.name for i in table.schema().fields], self.latest_node_id - 1)

    '''
    This is expected to be internal for now. This is a pretty new API and people probably don't know how to use this.
    '''
    def mingle(self, sources: dict, operator, new_schema: list, required_cols: dict):

        assert self.io_per_node == self.exec_per_node, "mingle currently only supports 1 to 1 mapping of IO to exec nodes, since we don't identify data sources"
        return self.new_stream(
                sources=sources,
                partitioners={k: PassThroughPartitioner() for k in sources},
                node=StatefulNode(
                    schema=new_schema,
                    schema_mapping={col: {-1: col} for col in new_schema},
                    required_columns=required_cols,
                    operator= operator),
                schema=new_schema)

    '''
    this is expected to be internal API, well internal as much as it can I guess until the syntactic sugar runs out.
    Although one should not expect average users to know how to use this API without shooting themselves in the foot.
    sources: Dict[Int -> DataStream]. The key is the identifier of the DataStream that the operator in your Node expects.
        i.e. if you Node has an operator that takes two streams identified by 0 and 1, then your keys here will be 0 or 1. 
        Again this is quite confusing and not expected to be used correctly by general population.
    partitioners: Dict[Int -> Partitioner]. The partitioning strategy for each input DataStream to this node. The value is one of the
        Partitioner classes in target_info.py
    node: A Node object, the logical plan node you plan to add here that processes the input DataStreams. 
    schema: a list of column names. This might be changed to a dictionary with type information in the future. 
    ordering: currently this is not supported. If ordering is specified, then the Quokka runtime will ensure that one input stream
        is entirely drained by this node before the other input stream starts to be processed. This can be used to implement build-probe join, e.g.
        implementing this is a priority.
    '''
    def new_stream(self, sources: dict, partitioners: dict, node, schema: list, sorted = None, materialized = False):
        self.nodes[self.latest_node_id] = node
        for source in sources:
            source_datastream = sources[source]
            node.parents[source] = source_datastream.source_node_id
            parent = self.nodes[source_datastream.source_node_id]
            parent.targets[self.latest_node_id] = TargetInfo(
                partitioners[source], sqlglot.exp.TRUE, None, [])

        self.latest_node_id += 1

        return DataStream(self, schema, self.latest_node_id - 1, sorted, materialized)

    '''
    This defines a dataset object which is used by the optimizer. 
    '''
    def new_dataset(self, source, schema: list):
        stream = self.new_stream(sources={0: source}, partitioners={
                                 0: PassThroughPartitioner()}, node=DataSetNode(schema), schema=schema)
        return DataSet(self, schema, stream.source_node_id)

    def optimize(self, node_id):
        self.__push_filter__(node_id)
        self.__early_projection__(node_id)
        self.__fold_map__(node_id)
        if self.sql_config["optimize_joins"]:
            self.__merge_joins__(node_id)
        self.__propagate_cardinality__(node_id)
        self.__determine_stages__(node_id)
        
        assert len(self.execution_nodes[node_id].parents) == 1
        parent_idx = list(self.execution_nodes[node_id].parents)[0]
        parent_id = self.execution_nodes[node_id].parents[parent_idx]

        if issubclass(type(self.execution_nodes[parent_id]), SourceNode):
            self.execution_nodes[node_id].blocking = True
            return node_id
        else:
            del self.execution_nodes[node_id]
            self.execution_nodes[parent_id].blocking = True
            return parent_id


    def lower(self, end_node_id, collect = True, dataset_schema = None):

        # start = time.time()
        task_graph = TaskGraph(self)
        node = self.execution_nodes[end_node_id]
        nodes = deque([node])
        reverse_sorted_nodes = [(end_node_id,node)]
        while len(nodes) > 0:
            new_node = nodes.popleft()
            for parent_idx in new_node.parents:
                parent_id = new_node.parents[parent_idx]
                reverse_sorted_nodes.append((parent_id,self.execution_nodes[parent_id]))
                nodes.append(self.execution_nodes[parent_id])
        reverse_sorted_nodes = reverse_sorted_nodes[::-1]
        task_graph_nodes = {}

        for node_id, node in reverse_sorted_nodes:
            if issubclass(type(node), SourceNode):
                task_graph_nodes[node_id] = node.lower(task_graph)
            else:
                parent_nodes = {parent_idx: task_graph_nodes[node.parents[parent_idx]] for parent_idx in node.parents}
                target_info = {parent_idx: self.execution_nodes[node.parents[parent_idx]].targets[node_id] for parent_idx in node.parents}
                task_graph_nodes[node_id] = node.lower(task_graph, parent_nodes, target_info)

        task_graph.create()
        # print("init time ", time.time() - start)
        start = time.time()
        task_graph.run()
        print("run time ", time.time() - start)
        result = task_graph_nodes[end_node_id]
        # wipe the execution state
        self.execution_nodes = {}
        if collect:
            return ray.get(self.dataset_manager.to_df.remote(result))
        else:
            assert dataset_schema is not None
            return Dataset(dataset_schema, self.dataset_manager, result)
                    

    def execute_node(self, node_id, explain = False, mode = None, collect = True):
        assert issubclass(type(self.nodes[node_id]), SinkNode)

        # we will now make a copy of the nodes involved in the computation. 
        
        end_schema = self.nodes[node_id].schema
        node = self.nodes[node_id]
        nodes = deque([node])
        self.execution_nodes = {node_id: copy.deepcopy(node)}

        while len(nodes) > 0:
            new_node = nodes.popleft()
            for parent_idx in new_node.parents:
                parent_id = new_node.parents[parent_idx]
                self.execution_nodes[parent_id] = copy.deepcopy(self.nodes[parent_id])
                nodes.append(self.nodes[parent_id])
        
        # prune targets from execution nodes that are not related to this execution
        execution_node_set = set(self.execution_nodes.keys())
        for execute_node_id in self.execution_nodes:
            node = self.execution_nodes[execute_node_id]
            new_targets = {}
            for target_id in node.targets:
                # this target is related to some other execution plan, don't have to be included here.
                if target_id in execution_node_set:
                    # only deleting the target in the node in execution_nodes, not in nodes!
                    new_targets[target_id] = node.targets[target_id]
            
            if len(new_targets) > 1:
                raise Exception("Currently a DataStream can only have one downstream consumer, please file an issue!")
            node.targets = new_targets

        
        #self.explain(node_id)

        new_node_id = self.optimize(node_id)       
        #self.explain(new_node_id)
    
        if explain:
            self.explain(new_node_id, mode = mode)
            return None
        else:
            return self.lower(new_node_id, collect = collect, dataset_schema = end_schema)

    def explain(self, node_id, mode="graph"):

        if mode == "text":
            print(node_id, self.execution_nodes[node_id])
            for parent in self.execution_nodes[node_id].parents:
                self.explain(self.execution_nodes[node_id].parents[parent], mode="text")
        else:
            logical_plan_graph = graphviz.Digraph(
                'logical-plan', node_attr={'shape': 'box'})
            logical_plan_graph.graph_attr['rankdir'] = 'BT'
            logical_plan_graph.node(str(node_id), str(node_id) + str(self.execution_nodes[node_id]))
            for parent in self.execution_nodes[node_id].parents:
                self._walk(self.execution_nodes[node_id].parents[parent], logical_plan_graph)
                try:
                    cardinality = self.execution_nodes[self.execution_nodes[node_id].parents[parent]].cardinality[node_id]
                    cardinality = round(cardinality, 2) if cardinality is not None else None
                except:
                    cardinality = None
                logical_plan_graph.edge(
                    str(self.execution_nodes[node_id].parents[parent]), str(node_id), label = str(cardinality))
            try:
                logical_plan_graph.view()
            except:
                print("Logical plan could not be viewed, most likely because you are running .explain() on AWS instance without proper X tunneling.")

    def _walk(self, node_id, graph):
        graph.node(str(node_id), str(node_id) + " " + str(self.execution_nodes[node_id]))
        for parent in self.execution_nodes[node_id].parents:
            self._walk(self.execution_nodes[node_id].parents[parent], graph)
            try:
                cardinality = self.execution_nodes[self.execution_nodes[node_id].parents[parent]].cardinality[node_id]
                cardinality = round(cardinality, 2) if cardinality is not None else None
            except:
                cardinality = None
            graph.edge(str(self.execution_nodes[node_id].parents[parent]), str(node_id), label = str(cardinality))

    def __push_filter__(self, node_id):

        node = self.execution_nodes[node_id]
        targets = node.targets

        # you are the one that triggered execution, you must be a SinkNode!
        if len(targets) == 0:
            for parent in node.parents:
                self.__push_filter__(node.parents[parent])
            return

        # if this node has more than one target, just give up, we might handle this later by pushing an OR predicate
        elif len(targets) > 1:
            for parent in node.parents:
                self.__push_filter__(node.parents[parent])
            return

        # you have exactly one target
        else:
            target_id = list(targets.items())[0][0]
            predicate = targets[target_id].predicate

            assert predicate == sqlglot.exp.TRUE or optimizer.normalize.normalized(
                predicate), "predicate must be CNF"

            # if this is a filter node, you will have exactly one parent.
            # you will rewire your parents targets to your targets, and delete yourself, as well as yourself from parent.targets
            # and target.parents for each of your targets

            if issubclass(type(node), SourceNode):
                # push down predicates to the Parquet Nodes!, for the CSV nodes give up
                if type(node) == InputDiskParquetNode or type(node) == InputS3ParquetNode or type(node) == InputS3IcebergNode:
                    filters, remaining_predicate = sql_utils.parquet_condition_decomp(predicate)
                    if len(filters) > 0:
                        node.predicate = filters
                        node.targets[target_id].predicate = optimizer.simplify.simplify(remaining_predicate)
                    return
                else:
                    return

            elif issubclass(type(node), FilterNode):
                predicate = optimizer.simplify.simplify(
                    sqlglot.exp.and_(predicate, node.predicate))
                parent_id = node.parents[0]
                parent = self.execution_nodes[parent_id]

                if target_id in parent.targets:
                    raise Exception("node with two targets detected, we should never have let the user created this graph!")

                parent.targets[target_id] = copy.deepcopy(
                    targets[target_id])
                parent.targets[target_id].and_predicate(predicate)
                success = False
                # we need to find which parent in the target is this node, and replace it with this node's parent
                for key in self.execution_nodes[target_id].parents:
                    if self.execution_nodes[target_id].parents[key] == node_id:
                        self.execution_nodes[target_id].parents[key] = parent_id
                        success = True
                        break
                assert success
                del parent.targets[node_id]
                del self.execution_nodes[node_id]
                return self.__push_filter__(parent_id)

            # if this is not a filter node, it might have multiple parents. This is okay.
            # we assume the predicate is in CNF format. We will go through all the conjuncts and determine which parent we can push each conjunct to

            else:
                if optimizer.simplify.simplify(predicate) == sqlglot.exp.TRUE:
                    for parent in node.parents:
                        self.__push_filter__(node.parents[parent])
                    return
                else:
                    conjuncts = list(
                        predicate.flatten()
                        if isinstance(predicate, sqlglot.exp.And)
                        else [predicate]
                    )
                    new_conjuncts = []
                    for conjunct in conjuncts:
                        
                        columns = set(i.name for i in conjunct.find_all(
                            sqlglot.expressions.Column))
                        conjunct_schema_mappings = { col : node.schema_mapping[col] for col in columns }

                        # each value here will be a dict of parent ids that have this column and what this column is called in the parent
                        conjunct_parents = {col: set(conjunct_schema_mappings[col].keys()) for col in columns}
                        # we need to make sure that all the values in conjunct_parents are the same, otherwise we cannot push this predicate down
                        parents = conjunct_parents[list(conjunct_parents.keys())[0]]
                        
                        if all([parents == conjunct_parents[col] for col in conjunct_parents]) and -1 not in parents:
                            for parent in parents:
                                copied_conjunct = copy.deepcopy(conjunct)
                                rename_dict = {col: node.schema_mapping[col][parent] for col in columns if col != node.schema_mapping[col][parent]}
                                for identifier in copied_conjunct.find_all(sqlglot.exp.Identifier):
                                    if identifier.name in rename_dict:
                                        identifier.replace(sqlglot.exp.to_identifier(rename_dict[identifier.name]))
                                parent_id = node.parents[parent]
                                parent = self.execution_nodes[parent_id]
                                parent.targets[node_id].and_predicate(copied_conjunct)
                        else:
                            new_conjuncts.append(conjunct)
                    predicate = sqlglot.exp.TRUE
                    for conjunct in new_conjuncts:
                        predicate = sqlglot.exp.and_(predicate, conjunct)
                    predicate = optimizer.simplify.simplify(predicate)
                    targets[target_id].predicate = predicate

                    for parent in node.parents:
                        self.__push_filter__(node.parents[parent])
                    return 

    def __early_projection__(self, node_id):

        node = self.execution_nodes[node_id]
        targets = node.targets

        if issubclass(type(node), SourceNode):
            # push down predicates to the Parquet Nodes! It benefits CSV nodes too because believe it or not polars.from_arrow could be slow
            if type(node) == InputDiskParquetNode or type(node) == InputS3ParquetNode or type(node) == InputDiskCSVNode or type(node) == InputS3CSVNode or type(node) == InputS3IcebergNode:
                projection = set()
                predicate_required_columns = set()
                for target_id in targets:
                    # can no longer push down any projections because one of the targets require all the columns
                    if targets[target_id].projection is None:
                        return
                    projection = projection.union(
                        targets[target_id].projection)
                    predicate_required_columns = predicate_required_columns.union(
                        targets[target_id].predicate_required_columns())
                
                # the node.required_columns for this input node is the union of the required columns of 
                node.projection = projection.union(predicate_required_columns)
                
                # still do the extra projection to ensure columns appear in the right order.
                #for target_id in targets:
                #    if targets[target_id].projection == node.projection:
                #        targets[target_id].projection = None

                return
            else:
                return
        # you are the one that triggered execution, you must be a SinkNode!
        elif len(targets) == 0:
            for parent in node.parents:
                self.__early_projection__(node.parents[parent])
        else:
            # you should have the required_columns attribute

            if issubclass(type(node), ProjectionNode):
                parent = self.execution_nodes[node.parents[0]]
                projection = set()
                predicate_required_columns = set()
                for target_id in targets:
                    target = targets[target_id]
                    # all the predicates should have been pushed past projection nodes in predicate pushdown
                    assert target.predicate == sqlglot.exp.TRUE, target.predicate
                    if target.projection is None:
                        target.projection = node.projection
                    # your target projections should never contain columns that you don't contain, should be asserted at DataStream level
                    assert set(target.projection).issubset(
                        set(node.projection))
                    # if your parent for some reason had some projection, your projection must be in a subset. This should also be asserted at DataStream level.
                    if parent.targets[node_id].projection is not None:
                        assert set(target.projection).issubset(
                            set(parent.targets[node_id].projection))

                    parent.targets[target_id] = TargetInfo(
                        target.partitioner, parent.targets[node_id].predicate, target.projection, target.batch_funcs)

                    success = False
                    # we need to find which parent in the target is this node, and replace it with this node's parent
                    for key in self.execution_nodes[target_id].parents:
                        if self.execution_nodes[target_id].parents[key] == node_id:
                            self.execution_nodes[target_id].parents[key] = node.parents[0]
                            success = True
                            break
                    assert success

                del parent.targets[node_id]
                del self.execution_nodes[node_id]
                return self.__early_projection__(node.parents[0])
            else:
                projection = set()
                predicate_required_columns = set()
                for target_id in targets:
                    # can no longer push down any projections because one of the targets require all the columns
                    if targets[target_id].projection is None:
                        projection = set(node.schema)
                        break
                    projection = projection.union(
                        targets[target_id].projection)
                    predicate_required_columns = predicate_required_columns.union(
                        targets[target_id].predicate_required_columns())

                # predicates may change due to predicate pushdown. This doens't change required_column attribute, which is the required columns for an operator
                # predicate_required_columns is recomputed at this stage. Those are added to the projection columns

                pushable_projections = projection.union(
                    predicate_required_columns)
                pushed_projections = {}

                # first make sure you include the operator required columns
                for parent_idx in self.execution_nodes[node_id].required_columns:
                    parent_id = node.parents[parent_idx]
                    pushed_projections[parent_id] = self.execution_nodes[node_id].required_columns[parent_idx]

                # figure out which parent each pushable column came from
                for col in pushable_projections:
                    parent_dict = node.schema_mapping[col]

                    # this column is generated from this node you can't push this beyond yourself
                    if -1 in parent_dict:
                        continue

                    for parent_idx in parent_dict:
                        parent_col = parent_dict[parent_idx]
                        parent_id = node.parents[parent_idx]
                        if parent_id in pushed_projections:
                            pushed_projections[parent_id].add(parent_col)
                        else:
                            pushed_projections[parent_id] = {parent_col}

                for parent_idx in node.parents:
                    parent_id = node.parents[parent_idx]
                    parent = self.execution_nodes[parent_id]
                    if parent_id in pushed_projections:
                        # if for some reason the parent's projection is not None then it has to contain whatever you are already projecting, or else that specified projection is wrong
                        if parent.targets[node_id].projection is not None:
                            assert pushed_projections[parent_id].issubset(
                                set(parent.targets[node_id].projection))

                        parent.targets[node_id].projection = pushed_projections[parent_id]
                    self.__early_projection__(parent_id)

    def __fold_map__(self, node_id):

        node = self.execution_nodes[node_id]
        targets = node.targets

        if issubclass(type(node), SourceNode):
            return
        # you are the one that triggered execution, you must be a SinkNode!
        elif len(targets) == 0:
            for parent in node.parents:
                self.__fold_map__(node.parents[parent])
        else:
            # you should have the required_columns attribute
            if issubclass(type(node), MapNode):

                if not node.foldable:  # this node should not be folded
                    return self.__fold_map__(node.parents[0])

                parent = self.execution_nodes[node.parents[0]]
                for target_id in targets:
                    target = targets[target_id]
                    if target.predicate == sqlglot.exp.TRUE:
                        parent.targets[target_id] = TargetInfo(target.partitioner, parent.targets[node_id].predicate,  target.projection, [
                                                            node.function] + target.batch_funcs)
                    else:
                        func = lambda x: x.filter(sql_utils.evaluate(target.predicate))
                        parent.targets[target_id] = TargetInfo(target.partitioner, parent.targets[node_id].predicate,  target.projection, [
                                                            node.function, func] + target.batch_funcs)

                    # we need to find which parent in the target is this node, and replace it with this node's parent
                    success = False
                    for key in self.execution_nodes[target_id].parents:
                        if self.execution_nodes[target_id].parents[key] == node_id:
                            self.execution_nodes[target_id].parents[key] = node.parents[0]
                            success = True
                            break
                    assert success

                del parent.targets[node_id]
                del self.execution_nodes[node_id]
                return self.__fold_map__(node.parents[0])
            else:
                for parent_idx in node.parents:
                    parent_id = node.parents[parent_idx]
                    self.__fold_map__(parent_id)
                return

    def __merge_joins__(self, node_id):

        # the goal of this pass is to merge join nodes into a virtual multi join node and then 
        # relower the multi join node into a series of join nodes.
        # we need to first perform a DFS traversal to find all the join nodes
        # and merge them along the way
        # we also need to handle the predicates and projections along the way. This pass is done 
        # after projection and predicate pushdown.

        node = self.execution_nodes[node_id]
        targets = node.targets
        parents = node.parents # this is going to be a dictionary of things

        if issubclass(type(node), SourceNode):
            return
        # you are the one that triggered execution, you must be a SinkNode!
        elif len(targets) == 0:
            for parent in node.parents:
                self.__merge_joins__(node.parents[parent])
            return
        else:
            # you should have the required_columns attribute
            if issubclass(type(node), JoinNode):

                # if you have more than one target or you are not an inner join, you can't be merged into a multi join
                if len(targets) > 1 or not all([join_spec[0] == "inner" for join_spec in node.join_specs]):
                    for parent in parents:
                        self.__merge_joins__(parents[parent])
                    return

                target_id = list(targets.keys())[0]

                # you have one target, you can be fused. first figure out how many of your parents are join nodes.
                
                while True:
                    new_parents = {-1: None}
                    not_done = False
                    old_join_specs = copy.deepcopy(node.join_specs)
                    new_join_specs = []
                    source_mapping = {}
                    for parent in parents:
                        parent_node = self.execution_nodes[parents[parent]]
                        if issubclass(type(parent_node), JoinNode) and len(parent_node.targets) == 1 and len(parent_node.targets[node_id].batch_funcs) == 0 and all([join_spec[0] == "inner" for join_spec in parent_node.join_specs]):
                            not_done = True
                            assert len(parent_node.join_specs) == 1
                            # you are now responsible for your parent's join spec
                            new_join_spec = {}
                            new_join_type = parent_node.join_specs[0][0]
                            # get rid of this parent and absorb it into yourself
                            new_keys = []
                            for key in parent_node.parents:
                                new_key = max(new_parents.keys()) + 1
                                new_keys.append(new_key)
                                new_parents[new_key] = parent_node.parents[key]
                                new_join_spec[new_key] = parent_node.join_specs[0][1][key]
                                # now we have to remove the parent node_id from the parent's parents' targets and replace it with this node's id
                                self.execution_nodes[parent_node.parents[key]].targets[node_id] = self.execution_nodes[parent_node.parents[key]].targets[parents[parent]]
                                del self.execution_nodes[parent_node.parents[key]].targets[parents[parent]]
                            source_mapping[parent] = new_keys

                            new_join_specs.append((new_join_type, new_join_spec))
                            # did the parent have predicates?
                            if parent_node.targets[node_id].predicate is not None:
                                if node.targets[target_id].predicate is None:
                                    node.targets[target_id].predicate = parent_node.targets[node_id].predicate
                                else:
                                    node.targets[target_id].predicate = optimizer.simplify.simplify(sqlglot.exp.and_(
                                        node.targets[target_id].predicate, parent_node.targets[node_id].predicate))
                            
                            del self.execution_nodes[parents[parent]]
                            # don't have to worry about the parent's projections. You only care about your projections, 
                            # i.e. the one you need at the very end.
                        else:
                            new_parent_key = max(new_parents.keys()) + 1
                            new_parents[new_parent_key] = parents[parent]
                            source_mapping[parent] = new_parent_key
                    
                    renamed_join_specs = []
                    for join_spec in old_join_specs:
                        new_join_spec = {}
                        for key in join_spec[1]:
                            new_key = source_mapping[key]
                            if type(new_key) == list:
                                # need to figure out which one of the new keys this key belongs to
                                # TODO: in case this new key belongs to both, it will default to the first one
                                # eventually you want to move to a join graph. Eventually.
                                join_key = join_spec[1][key]
                                assert len(new_key) == 2
                                if join_key in self.execution_nodes[new_parents[new_key[0]]].targets[node_id].projection:
                                    new_key = new_key[0]
                                else:
                                    new_key = new_key[1]
                            new_join_spec[new_key] = join_spec[1][key]
                        renamed_join_specs.append((join_spec[0], new_join_spec))
                    node.join_specs = renamed_join_specs + new_join_specs
                    
                    del new_parents[-1]
                    parents = new_parents
                    if not not_done:
                        break
                    # print(node.join_specs)
                
                node.parents = parents

                for parent in parents:
                    self.__merge_joins__(parents[parent])
                return

            else:
                for parent_idx in node.parents:
                    parent_id = node.parents[parent_idx]
                    self.__merge_joins__(parent_id)
                return
    
    def __propagate_cardinality__(self, node_id):

        node = self.execution_nodes[node_id]
        parents = node.parents

        if issubclass(type(node), SourceNode):
            node.set_cardinality(self.catalog)
            return
        else:
            for parent in parents:
                self.__propagate_cardinality__(parents[parent])
            parent_cardinalites = {parent: self.execution_nodes[parents[parent]].cardinality[node_id] for parent in parents}
            node.set_cardinality(parent_cardinalites)

    # this is the pass that fixes the join order. Arguably the MOST important pass
    def __determine_stages__(self, node_id):

        node = self.execution_nodes[node_id]
        targets = node.targets
        parents = node.parents # this is going to be a dictionary of things

        # the main logic assumes that the node at node_id has been already assigned a stage
        # and is in charge of assigning stages to its parents. If you are the sink node this won't be true
        # so you have to assign yourself a stage of 0
        if len(targets) == 0:
            node.assign_stage(0)

        if issubclass(type(node), SourceNode):
            # you should already have been assigned by your target
            return
        else:
            if issubclass(type(node), JoinNode):
                
                # check if everything is an inner join

                if all([join_spec[0] == "inner" for join_spec in node.join_specs]):
                    estimated_cardinality = {}
                    for parent in parents:
                        estimated_cardinality[parent] = self.execution_nodes[parents[parent]].cardinality[node_id]
                        if estimated_cardinality[parent] is None:
                            estimated_cardinality[parent] = -1

                    # set probe to be the parent with the largest cardinality
                    probe = max(estimated_cardinality, key=estimated_cardinality.get)
                    self.execution_nodes[parents[probe]].assign_stage(node.stage)
                    for parent in parents:
                        if parent != probe:
                            self.execution_nodes[parents[parent]].assign_stage(node.stage - 1)
                    
                    # you are now responsible for arranging the joinspec in the right order!
                    # you should have a join_specs attribute
                    new_join_specs = []
                    old_join_specs = set(range(len(node.join_specs)))
                    existing_tables = {probe}

                    while len(old_join_specs) > 0:

                        # find the build side that's smallest to join against.

                        candidates ={ index: list(set(node.join_specs[index][1].keys()).difference(existing_tables)) for index in old_join_specs if len(set(node.join_specs[index][1].keys()).intersection(existing_tables)) > 0}
                        assert all([len(candidates[index]) == 1 for index in candidates]), "There is a duplicate join condition, blowing up"
                        candidates = {index: candidates[index][0] for index in candidates}
                        assert len(candidates) > 0, "discontiguous join conditions. Blowing up"
                        candidate = min(candidates, key=lambda index: estimated_cardinality[candidates[index]])
                        other_table = candidates[candidate]
                        intersected_table = list(set(node.join_specs[candidate][1].keys()).intersection(existing_tables))[0]
                        join_spec = node.join_specs[candidate]

                        new_join_spec = (join_spec[0], [(intersected_table, join_spec[1][intersected_table]), (other_table, join_spec[1][other_table])])
                        new_join_specs.append(new_join_spec)
                        old_join_specs.remove(candidate)
                        existing_tables = existing_tables.union(set(join_spec[1].keys()))

                    node.join_specs = new_join_specs
                    # print(new_join_specs)
                else:
                    # there are some joins that are not inner joins 
                    # currently we do not attempt to reorder them, in fact we can only tolerate one join here. haha.
                    assert len(node.join_specs) == 1, "There are multiple joins and some of them are not inner joins, blowing up"
                    assert len(parents) == 2 and 0 in parents and 1 in parents
                    join_spec = node.join_specs[0]
                    # in a semi, anti or left join, 0 needs to be probe.
                    node.join_specs = [(join_spec[0], [(0, join_spec[1][0]), (1, join_spec[1][1])])] 
                    self.execution_nodes[parents[0]].assign_stage(node.stage)
                    self.execution_nodes[parents[1]].assign_stage(node.stage - 1)

            
            else:
                # for other nodes we currently don't do anything
                for parent in parents:
                    self.execution_nodes[parents[parent]].assign_stage(node.stage)
        
        for parent in parents:
            self.__determine_stages__(parents[parent])

class DataSet:
    def __init__(self, quokka_context: QuokkaContext, schema: dict, source_node_id: int) -> None:
        self.quokka_context = quokka_context
        self.schema = schema
        self.source_node_id = source_node_id
