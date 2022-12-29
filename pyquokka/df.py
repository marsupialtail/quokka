import graphviz
import copy
import polars
from pyquokka.logical import InputDiskFilesNode, InputS3FilesNode, SourceNode
import pyquokka.sql_utils as sql_utils
from pyquokka.datastream import * 
import os

class QuokkaContext:
    def __init__(self, cluster = None, io_per_node = 2, exec_per_node = 1) -> None:
        self.latest_node_id = 0
        self.nodes = {}
        self.cluster = LocalCluster() if cluster is None else cluster
        self.io_per_node = io_per_node
        self.exec_per_node = exec_per_node

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
        or CSV(s) on S3. Currently other cloud sare not supported. The CSVs can have a predefined schema using a list of 
        column names in the schema argument, or you can specify the CSV has a header row and Quokka will read the schema 
        from it. You should also specify the CSV's separator. 

        Args:
            table_location (str): where the CSV(s) are. This mostly mimics Spark behavior. Look at the examples.
            schema (list): you can provide a list of column names, it's kinda like polars.read_csv(new_columns=...)
            has_header (bool): is there a header row. If the schema is not provided, this should be True. If the schema IS provided, 
                this can still be True. Quokka will just ignore the header row.
            sep (str): default to ',' but could be something else, like '|' for TPC-H tables

        Return:
            A new DataStream if the CSV file is larger than 10MB, otherwise a Polars DataFrame. 
        
        Examples:
            ~~~python
            # read a single CSV. It's better always to specify the absolute path.
            >>> lineitem = qc.read_csv("/home/ubuntu/tpch/lineitem.csv")

            # read a directory of CSVs 
            >>> lineitem = qc.read_csv("/home/ubuntu/tpch/lineitem/*")

            # read a single CSV from S3
            >>> lineitem = qc.read_csv("s3://tpc-h-csv/lineitem/lineitem.tbl.1")

            # read CSVs from S3 bucket with prefix
            >>> lineitem = qc.read_csv("s3://tpc-h-csv/lineitem/*")
            ~~~
        """

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
                files = [i['Key'] for i in z['Contents']]
                sizes = [i['Size'] for i in z['Contents']]
                assert len(files) > 0

                if schema is None:
                    resp = s3.get_object(
                        Bucket=bucket, Key=files[0], Range='bytes={}-{}'.format(0, 4096))['Body'].read()
                    first_newline = resp.find(bytes('\n', 'utf-8'))
                    if first_newline == -1:
                        raise Exception("could not detect the first line break with first 4 kb")
                    schema = resp[:first_newline].decode("utf-8").split(sep)

                if len(files) == 1 and sizes[0] < 10 * 1048576:
                    return polars.read_csv("s3://" + bucket + "/" + files[0], new_columns = schema, has_header = has_header,sep = sep)

                self.nodes[self.latest_node_id] = InputS3CSVNode(bucket, prefix, None, schema, sep, has_header)
            else:
                key = "/".join(table_location.split("/")[1:])
                s3 = boto3.client('s3')
                response = s3.head_object(Bucket= bucket, Key=key)
                size = response['ContentLength']
                if size < 10 * 1048576:
                    return polars.read_csv("s3://" + table_location, new_columns = schema, has_header = has_header,sep = sep)
                else:

                    if schema is None:
                        resp = s3.get_object(
                            Bucket=bucket, Key=key, Range='bytes={}-{}'.format(0, 4096))['Body'].read()
                        first_newline = resp.find(bytes('\n', 'utf-8'))
                        if first_newline == -1:
                            raise Exception("could not detect the first line break with first 4 kb")
                        schema = resp[:first_newline].decode("utf-8").split(sep)

                    self.nodes[self.latest_node_id] = InputS3CSVNode(bucket, None, key, schema, sep, has_header)
            # self.nodes[self.latest_node_id].set_placement_strategy(CustomChannelsStrategy(2))
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
                if len(files) == 1:
                    size = os.path.getsize(table_location + files[0])
                    if size < 10 * 1048576:
                        return polars.read_csv(table_location + files[0], new_columns=schema, has_header=has_header, sep = sep)
                
                if schema is None:
                    resp = open(table_location + files[0],"r").read(1024 * 4)
                    first_newline = resp.find("\n")
                    if first_newline == -1:
                        raise Exception("could not detect the first line break within the first 4 kb")
                    schema = resp[:first_newline].split(sep)

                self.nodes[self.latest_node_id] = InputDiskCSVNode(table_location, schema, sep, has_header)
            else:
                size = os.path.getsize(table_location)
                if size < 10 * 1048576:
                    return polars.read_csv(table_location, new_columns = schema, has_header = has_header,sep = sep)
                else:
                    
                    if schema is None:
                        resp = open(table_location,"r").read(1024 * 4)
                        first_newline = resp.find("\n")
                        if first_newline == -1:
                            raise Exception("could not detect the first line break within the first 4 kb")
                        schema = resp[:first_newline].split(sep)

                    self.nodes[self.latest_node_id] = InputDiskCSVNode(table_location, schema, sep, has_header)
            
            # if local, you should not launch too many actors since not that many needed to saturate disk
            # self.nodes[self.latest_node_id].set_placement_strategy(CustomChannelsStrategy(2))

        self.latest_node_id += 1
        return DataStream(self, schema, self.latest_node_id - 1)

    '''
    The API layer for read_parquet does four things:
    - Detect if it's a S3 location of a local location
    - Detect if it's a list of files or a single file
    - Detect the schema. This is not expected to be supplied by the user. This is not needed by the reader but is needed by the logical plan optimizer.
    - Detect if the data is small enough (< 10MB and single file) to be immediately materialized into a Polars dataframe.
    After it has done these things, if the dataset is not materialized, we will instantiate a logical plan node and return a DataStream
    '''

    def read_parquet(self, table_location: str, schema = None):

        """
        Read Parquet. It can be a single Parquet or a list of Parquets. It can be Parquet(s) on disk
        or Parquet(s) on S3. Currently other cloud sare not supported. You don't really have to supply the schema
        since you can get it from the metadata always, but you can if you want.

        Args:
            table_location (str): where the Parquet(s) are. This mostly mimics Spark behavior. Look at the examples.
            schema (list): list of column names. This is optional. If you do supply it, please make sure it's correct!

        Return:
            A new DataStream if the Parquet file is larger than 10MB, otherwise a Polars DataFrame. 
        
        Examples:
            ~~~python
            # read a single Parquet. It's better always to specify the absolute path.
            >>> lineitem = qc.read_parquet("/home/ubuntu/tpch/lineitem.parquet")

            # read a directory of Parquets 
            >>> lineitem = qc.read_parquet("/home/ubuntu/tpch/lineitem/*")

            # read a single Parquet from S3
            >>> lineitem = qc.read_parquet("s3://tpc-h-parquet/lineitem.parquet")

            # read Parquets from S3 bucket with prefix
            >>> lineitem = qc.read_parquet("s3://tpc-h-parquet/lineitem/*")
            ~~~
        """

        if table_location[:5] == "s3://":

            if type(self.cluster) == LocalCluster:
                print("Warning: trying to read S3 dataset on local machine. This assumes high network bandwidth.")

            table_location = table_location[5:]
            bucket = table_location.split("/")[0]
            if "*" in table_location:
                assert table_location[-1] == "*" , "wildcard can only be the last character in address string"
                table_location = table_location[:-1]
                assert "*" not in table_location, "wildcard can only be the last character in address string"
                prefix = "/".join(table_location[:-1].split("/")[1:])
                s3 = boto3.client('s3')
                z = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
                files = [i['Key'] for i in z['Contents'] if i['Key'].endswith(".parquet")]
                sizes = [i['Size'] for i in z['Contents'] if i['Key'].endswith('.parquet')]
                assert len(files) > 0, "could not find any parquet files. make sure they end with .parquet"
                if len(files) == 1 and sizes[0] < 10 * 1048576:
                    return polars.read_parquet("s3://" + bucket + "/" + files[0])
                
                if schema is None:
                    try:
                        s3 = S3FileSystem()
                        f = pq.ParquetFile(s3.open_input_file(bucket + "/" + files[0]))
                        schema = [k.name for k in f.schema_arrow]
                    except:
                        raise Exception("schema discovery failed for Parquet dataset at location ", table_location)
                
                self.nodes[self.latest_node_id] = InputS3ParquetNode(bucket, prefix, None, schema)
            else:
                if schema is None:
                    try:
                        s3 = S3FileSystem()
                        f = pq.ParquetFile(s3.open_input_file(table_location))
                        schema = [k.name for k in f.schema_arrow]
                    except:
                        raise Exception("schema discovery failed for Parquet dataset at location ", table_location)
                key = "/".join(table_location.split("/")[1:])
                s3 = boto3.client('s3')
                response = s3.head_object(Bucket= bucket, Key=key)
                size = response['ContentLength']
                if size < 10 * 1048576:
                    return polars.read_parquet("s3://" + table_location)
                else:
                    self.nodes[self.latest_node_id] = InputS3ParquetNode(bucket, None, key, schema)

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
                if schema is None:
                    f = pq.ParquetFile(table_location + files[0])
                    schema = [k.name for k in f.schema_arrow]
                if len(files) == 1:
                    size = os.path.getsize(table_location + files[0])
                    if size < 10 * 1048576:
                        return polars.read_parquet(table_location + files[0])
                self.nodes[self.latest_node_id] = InputDiskParquetNode(table_location, schema)

            else:
                try:
                    size = os.path.getsize(table_location)
                except:
                    raise Exception("could not find the parquet file at ", table_location)
                
                if size < 9 * 1048576:
                    return polars.read_parquet(table_location)
                else:
                    if schema is None:
                        f = pq.ParquetFile(table_location)
                        schema = [k.name for k in f.schema_arrow]
                    self.nodes[self.latest_node_id] = self.nodes[self.latest_node_id] = InputDiskParquetNode(table_location, schema)

            # if local, you should not launch too many actors since not that many needed to saturate disk
            # self.nodes[self.latest_node_id].set_placement_strategy(CustomChannelsStrategy(2))

        self.latest_node_id += 1
        return DataStream(self, schema, self.latest_node_id - 1)
    
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
                    schema_mapping={col: (-1, col) for col in new_schema},
                    required_columns=required_cols,
                    operator= operator),
                schema=new_schema,
                ordering=None)

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
    def new_stream(self, sources: dict, partitioners: dict, node: Node, schema: list, ordering=None):
        self.nodes[self.latest_node_id] = node
        for source in sources:
            source_datastream = sources[source]
            node.parents[source] = source_datastream.source_node_id
            parent = self.nodes[source_datastream.source_node_id]
            parent.targets[self.latest_node_id] = TargetInfo(
                partitioners[source], sqlglot.exp.TRUE, None, [])

        self.latest_node_id += 1

        return DataStream(self, schema, self.latest_node_id - 1)

    '''
    This defines a dataset object which is used by the optimizer. 
    '''
    def new_dataset(self, source, schema: list):
        stream = self.new_stream(sources={0: source}, partitioners={
                                 0: PassThroughPartitioner()}, node=DataSetNode(schema), schema=schema, ordering=None)
        return DataSet(self, schema, stream.source_node_id)

    def optimize(self, node_id):
        self.__push_filter__(node_id)
        self.__early_projection__(node_id)
        self.__fold_map__(node_id)
        
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


    def lower(self, end_node_id, collect = True):

        start = time.time()
        task_graph = TaskGraph(self.cluster, self.io_per_node, self.exec_per_node)
        # print("setup time ", time.time() - start)
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
        print("init time ", time.time() - start)
        start = time.time()
        task_graph.run()
        print("run time ", time.time() - start)
        result = task_graph_nodes[end_node_id]
        # wipe the execution state
        self.execution_nodes = {}
        if collect:
            return result.to_df()
        else:
            return result
                    

    def execute_node(self, node_id, explain = False, mode = None, collect = True):
        assert issubclass(type(self.nodes[node_id]), SinkNode)

        # we will now make a copy of the nodes involved in the computation. 
        
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
            node.targets = new_targets
        
        #self.explain(node_id)

        new_node_id = self.optimize(node_id)       
        #self.explain(new_node_id)
    
        if explain:
            self.explain(new_node_id, mode = mode)
            return None
        else:
            return self.lower(new_node_id, collect = collect)

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
                logical_plan_graph.edge(
                    str(self.execution_nodes[node_id].parents[parent]), str(node_id))
            logical_plan_graph.view()

    def _walk(self, node_id, graph):
        graph.node(str(node_id), str(self.execution_nodes[node_id]))
        for parent in self.execution_nodes[node_id].parents:
            self._walk(self.execution_nodes[node_id].parents[parent], graph)
            graph.edge(str(self.execution_nodes[node_id].parents[parent]), str(node_id))

    def __push_filter__(self, node_id):

        node = self.execution_nodes[node_id]
        targets = node.targets

        # you are the one that triggered execution, you must be a SinkNode!
        if len(targets) == 0:
            for parent in node.parents:
                self.__push_filter__(node.parents[parent])

        # if this node has more than one target, just give up, we might handle this later by pushing an OR predicate
        elif len(targets) > 1:
            for parent in node.parents:
                self.__push_filter__(node.parents[parent])

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
                if type(node) == InputDiskParquetNode or type(node) == InputS3ParquetNode:
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
                if optimizer.simplify.simplify(predicate) == sqlglot.exp.TRUE:
                    return self.__push_filter__(parent_id)
                else:
                    parent = self.execution_nodes[parent_id]
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
                        return self.__push_filter__(node.parents[parent])
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
                        parents = set(
                            node.schema_mapping[col][0] for col in columns)
                        # the schema mapping also tells you what this column is called in the parent
                        rename_dict = {col: node.schema_mapping[col][1] for col in columns if col != node.schema_mapping[col][1]}
                        # all the columns are from one parent, and not from yourself.
                        if len(parents) == 1 and -1 not in parents:
                            for identifier in conjunct.find_all(sqlglot.exp.Identifier):
                                if identifier.name in rename_dict:
                                    identifier.replace(sqlglot.exp.to_identifier(rename_dict[identifier.name]))
                            parent_id = node.parents[parents.pop()]
                            parent = self.execution_nodes[parent_id]
                            parent.targets[node_id].and_predicate(conjunct)
                        else:
                            new_conjuncts.append(conjunct)
                    predicate = sqlglot.exp.TRUE
                    for conjunct in new_conjuncts:
                        predicate = sqlglot.exp.and_(predicate, conjunct)
                    predicate = optimizer.simplify.simplify(predicate)
                    targets[target_id].predicate = predicate

                    for parent in node.parents:
                        self.__push_filter__(node.parents[parent])

    def __early_projection__(self, node_id):

        node = self.execution_nodes[node_id]
        targets = node.targets

        if issubclass(type(node), SourceNode):
            # push down predicates to the Parquet Nodes! It benefits CSV nodes too because believe it or not polars.from_arrow could be slow
            if type(node) == InputDiskParquetNode or type(node) == InputS3ParquetNode or type(node) == InputDiskCSVNode or type(node) == InputS3CSVNode:
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
                    assert target.predicate == sqlglot.exp.TRUE
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
                    parent_idx, parent_col = node.schema_mapping[col]

                    # this column is generated from this node you can't push this beyond yourself
                    if parent_idx == -1:
                        continue

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
                    parent.targets[target_id] = TargetInfo(target.partitioner, parent.targets[node_id].predicate,  target.projection, [
                                                           node.function] + target.batch_funcs)

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


class DataSet:
    def __init__(self, quokka_context: QuokkaContext, schema: dict, source_node_id: int) -> None:
        self.quokka_context = quokka_context
        self.schema = schema
        self.source_node_id = source_node_id
