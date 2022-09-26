import graphviz
import ray
import copy
from functools import partial
import pyarrow as pa
import polars

from pyquokka.executors import *
from pyquokka.dataset import *
from pyquokka.logical import *
from pyquokka.target_info import *
from pyquokka.quokka_runtime import * 
from pyquokka.utils import S3Cluster, LocalCluster
import pyquokka.sql_utils as sql_utils



class QuokkaContext:
    def __init__(self, cluster) -> None:
        self.latest_node_id = 0
        self.nodes = {}
        self.cluster = cluster

    def read_csv(self, table_location: str, schema, has_header = False, sep=","):

        # the reader is fine if you supply no schema, but the DataStream needs it for potential downstream filtering/pushdowns.
        # that's why we need the schema. TODO: We should be able to infer it, at least if the header is provided. This is future work. 

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
                if len(files) == 1 and sizes[0] < 10 * 1048576:
                    return polars.read_csv("s3://" + bucket + "/" + files[0])

                table_location = {"bucket":bucket, "prefix":prefix}
                self.nodes[self.latest_node_id] = InputS3CSVNode(bucket, prefix, None, schema, sep)
            else:
                key = "/".join(table_location.split("/")[1:])
                s3 = boto3.client('s3')
                response = s3.head_object(Bucket= bucket, Key=key)
                size = response['ContentLength']
                if size < 10 * 1048576:
                    return polars.read_csv(table_location, new_columns = schema, has_header = has_header,sep = sep)
                else:
                    self.nodes[self.latest_node_id] = InputS3CSVNode(bucket, None, key, schema, sep)
        else:

            if type(self.cluster) == S3Cluster:
                raise NotImplementedError("Does not support reading local dataset with S3 cluster. Must use S3 bucket.")

            if "*" in table_location:
                raise NotImplementedError("Currently does not support reading a directory of CSVs locally.")
            else:
                size = os.path.getsize(table_location)
                if size < 10 * 1048576:
                    return polars.read_csv(table_location, new_columns = schema, has_header = has_header,sep = sep)
                else:
                    self.nodes[self.latest_node_id] = InputDiskCSVNode(table_location, schema, sep)

        self.latest_node_id += 1
        return DataStream(self, schema, self.latest_node_id - 1)

    def read_parquet(self, table_location: str, schema = None):
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
                files = [i['Key'] for i in z['Contents'] if i['Key'].endswith(".parquet")]
                sizes = [i['Size'] for i in z['Contents'] if i['Key'].endswith('.parquet')]
                assert len(files) > 0
                if len(files) == 1 and size < 10 * 1048576:
                    return polars.read_parquet("s3://" + bucket + "/" + files[0])
                
                if schema is None:
                    try:
                        s3 = s3fs.S3FileSystem()
                        f = pq.ParquetFile(s3.open(bucket + "/" + files[0], "rb"))
                        schema = [k.name for k in f.schema_arrow]
                    except:
                        raise Exception("schema discovery failed for Parquet dataset at location ", table_location)
                
                table_location = {"bucket":bucket, "prefix":prefix}
                print(table_location)
                self.nodes[self.latest_node_id] = InputS3ParquetNode(bucket, prefix, None, schema)
            else:

                if schema is None:
                    try:
                        s3 = s3fs.S3FileSystem()
                        f = pq.ParquetFile(s3.open(table_location, "rb"))
                        schema = [k.name for k in f.schema_arrow]
                    except:
                        raise Exception("schema discovery failed for Parquet dataset at location ", table_location)
                key = "/".join(table_location.split("/")[1:])
                s3 = boto3.client('s3')
                response = s3.head_object(Bucket= bucket, Key=key)
                size = response['ContentLength']
                if size < 10 * 1048576:
                    return polars.read_parquet(table_location)
                else:
                    self.nodes[self.latest_node_id] = InputS3ParquetDataset(bucket, None, key, schema)

        else:
            if type(self.cluster) == S3Cluster:
                raise NotImplementedError("Does not support reading local dataset with S3 cluster. Must use S3 bucket.")

            self.nodes[self.latest_node_id] = InputDiskParquetNode(table_location, schema)

        self.latest_node_id += 1
        return DataStream(self, schema, self.latest_node_id - 1)

    # this is expected to be internal API, well internal as much as it can I guess until the syntactic sugar runs out.
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

    def new_dataset(self, source, schema: list):
        stream = self.new_stream(sources={0: source}, partitioners={
                                 0: PassThroughPartitioner()}, node=DataSetNode(schema), schema=schema, ordering=None)
        return DataSet(self, schema, stream.source_node_id)

    def optimize(self, node_id):
        self.__push_filter__(node_id)
        self.__early_projection__(node_id)
        self.__fold_map__(node_id)
        
        assert len(self.execution_nodes[node_id].parents) == 1
        parent_idx, parent_id = self.execution_nodes[node_id].parents.popitem()
        del self.execution_nodes[node_id]
        self.execution_nodes[parent_id].blocking = True
        return parent_id


    def lower(self, end_node_id):

        start = time.time()
        task_graph = TaskGraph(self.cluster)
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
                placement_strategy = node.placement_strategy
                if placement_strategy is None:
                    task_graph_nodes[node_id] = node.lower(task_graph, parent_nodes, target_info)
                elif type(placement_strategy) == SingleChannelStrategy:
                    task_graph_nodes[node_id] = node.lower(task_graph, parent_nodes, target_info, {self.cluster.leader_private_ip: 1})
                elif type(placement_strategy) == CustomChannelsStrategy:
                    task_graph_nodes[node_id] = node.lower(task_graph, parent_nodes, target_info,  
                        {ip: placement_strategy.channels_per_node for ip in list(self.cluster.private_ips.values())})
                elif type(placement_strategy) == GPUStrategy:
                    raise NotImplementedError
                else:
                    raise Exception("could not understand node placement strategy")
        
        task_graph.create()
        print("init time ", time.time() - start)
        start = time.time()
        task_graph.run()
        print("run time ", time.time() - start)
        result = task_graph_nodes[end_node_id].to_pandas()
        # wipe the execution state
        self.execution_nodes = {}
        return result
                    

    def execute_node(self, node_id):
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
        
        #self.explain(node_id,"text")

        new_node_id = self.optimize(node_id)       
        #self.explain(new_node_id)
        
        return self.lower(new_node_id)

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
            # push down predicates to the Parquet Nodes!, for the CSV nodes give up
            if type(node) == InputDiskParquetNode or type(node) == InputS3ParquetNode:
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
                for target_id in targets:
                    if targets[target_id].projection == node.projection:
                        targets[target_id].projection = None

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


class Partitioner:
    def __init__(self, ) -> None:
        pass


class DataStream:
    def __init__(self, quokka_context: QuokkaContext, schema: list, source_node_id: int) -> None:
        self.quokka_context = quokka_context
        self.schema = schema
        self.source_node_id = source_node_id

    def collect(self):
        """
        This will trigger the execution of computational graph, similar to Spark collect
        The result will be a Dataset, which you can then call to_pandas(), or use to_stream() to initiate another computation.
        """
        dataset = self.quokka_context.new_dataset(self, self.schema)
        return self.quokka_context.execute_node(dataset.source_node_id)
    
    #def explain(self):

    def write_csv(self, path):
        pass

    def write_parquet(self, path):
        pass

    def filter(self, predicate: str):

        predicate = sqlglot.parse_one(predicate)
        # convert to CNF
        predicate = optimizer.normalize.normalize(predicate)
        columns = set(i.name for i in predicate.find_all(
            sqlglot.expressions.Column))
        for column in columns:
            assert column in self.schema, "Tried to filter on a column not in the schema"

        return self.quokka_context.new_stream(sources={0: self}, partitioners={0: PassThroughPartitioner()}, node=FilterNode(self.schema, predicate),
                                              schema=self.schema, ordering=None)

    def select(self, columns: list):

        assert type(columns) == set or type(columns) == list

        for column in columns:
            assert column in self.schema, "Projection column not in schema"

        return self.quokka_context.new_stream(
            sources={0: self},
            partitioners={0: PassThroughPartitioner()},
            node=ProjectionNode(set(columns)),
            schema=columns,
            ordering=None)

    '''
    This is a rather Quokka-specific API that allows arbitrary transformations on a DataStream, similar to Spark RDD.map.
    Each RecordBatch outputted will be transformed according to a custom UDF, and the resulting RecordBatch is not 
    guaranteed to have the same length.

    Projections will not be able to be pushed through a MapNode created with transform because the schema mapping will indicate
    that all the columns are made by this node. However the transform API will first insert a ProjectioNode for the required columns.

    Predicates will not be able to be pushed through a TransformNode. 
    '''

    def transform(self, f, new_schema: list, required_columns: set, foldable=True):

        assert type(required_columns) == set

        select_stream = self.select(required_columns)

        return self.quokka_context.new_stream(
            sources={0: select_stream},
            partitioners={0: PassThroughPartitioner()},
            node=MapNode(
                schema=new_schema,
                schema_mapping={col: (-1, col) for col in new_schema},
                required_columns={0: required_columns},
                function=f,
                foldable=foldable
            ),
            schema=new_schema,
            ordering=None
        )

    '''
    This will create new columns from certain columns in the dataframe. 
    This is similar to pandas df.apply() that makes new columns. 
    This is a separate API because the semantics allow for projection and predicate pushdown through this node, since 
    the original columns are all preserved. 
    This is very similar to Spark df.withColumns, except this returns a new DataStream while Spark's version is inplace
    '''

    def with_column(self, new_column, f, required_columns = None, foldable=True, engine = "polars"):

        # it is the user's responsibility to specify what are the required columns! If the user doesn't specify anything,
        # it is assumed all columns are needed and projection pushdown becomes impossible.
        if required_columns is None:
            required_columns = set(self.schema)

        assert type(required_columns) == set
        assert new_column not in self.schema, "For now new columns cannot have same names as existing columns"
        assert engine == "polars" or engine == "pandas"

        def polars_func(func, batch):
            return batch.with_column(polars.Series(name=new_column, values = func(batch)))

        def pandas_func(func, batch):
            pandas_batch = batch.to_pandas()
            pandas_batch[new_column] = func(pandas_batch)
            return pa.RecordBatch.from_pandas(pandas_batch)

        return self.quokka_context.new_stream(
            sources={0: self},
            partitioners={0: PassThroughPartitioner()},
            node=MapNode(
                schema=self.schema+[new_column],
                schema_mapping={
                    **{new_column : (-1, new_column)}, **{col: (0, col) for col in self.schema}},
                required_columns={0: required_columns},
                function=partial(polars_func, f) if engine == "polars" else partial(pandas_func, f),
                foldable=foldable),
            schema=self.schema + [new_column],
            ordering=None)

    def distinct(self, keys: list):

        if type(keys) == str:
            keys = [keys]

        for key in keys:
            assert key in self.schema

        return self.quokka_context.new_stream(
            sources={0: self},
            partitioners={0: PassThroughPartitioner()},
            node=StatefulNode(
                schema=keys,
                # this is a stateful node, but predicates and projections can be pushed down.
                schema_mapping={col: (0, col) for col in keys},
                required_columns={0: set(keys)},
                operator=DistinctExecutor(keys)
            ),
            schema=keys,
            ordering=None
        )

    def join(self, right, on=None, left_on=None, right_on=None, suffix="_2", how="inner"):

        assert type(right) == polars.internals.frame.DataFrame or issubclass(type(right), DataStream)

        if on is None:
            assert left_on is not None and right_on is not None
            assert left_on in self.schema, "join key not found in left table"
            assert right_on in right.schema, "join key not found in right table"
        else:
            assert on in self.schema, "join key not found in left table"
            assert on in right.schema, "join key not found in right table"
            left_on = on
            right_on = on
            on = None

        # we can't do this check since schema is now a list of names with no type info. This should change in the future.
        #assert node1.schema[left_on] == node2.schema[right_on], "join column has different schema in tables"

        new_schema = self.schema.copy()
        schema_mapping = {col: (0, col) for col in self.schema}

        # if the right table is already materialized, the schema mapping should forget about it since we can't push anything down anyways.
        # an optimization could be to push down the predicate directly to the materialized polars table in the BroadcastJoinExecutor
        # leave this as a TODO. this could be greatly benenficial if it significantly reduces the size of the small table.
        if type(right) == polars.internals.frame.DataFrame:
            right_table_id = -1
        else:
            right_table_id = 1

        for col in right.schema:
            if col == right_on:
                continue
            if col in new_schema:
                assert col + suffix not in new_schema, ("the suffix was not enough to guarantee unique col names", col + suffix, new_schema)
                new_schema.append(col + suffix)
                schema_mapping[col+suffix] = (right_table_id, col)
            else:
                new_schema.append(col)
                schema_mapping[col] = (right_table_id, col)

        if issubclass(type(right), DataStream):
            return self.quokka_context.new_stream(
                sources={0: self, 1: right},
                partitioners={0: HashPartitioner(left_on), 1: HashPartitioner(right_on)},
                node=StatefulNode(
                    schema=new_schema,
                    schema_mapping=schema_mapping,
                    required_columns={0: {left_on}, 1: {right_on}},
                    operator=PolarJoinExecutor(on, left_on, right_on, suffix=suffix, how=how)),
                schema=new_schema,
                ordering=None)
        elif type(right) == polars.internals.frame.DataFrame:
            return self.quokka_context.new_stream(
                sources = {0:self},
                partitioners={0: PassThroughPartitioner()},
                node = StatefulNode(
                    schema = new_schema,
                    schema_mapping=schema_mapping,
                    required_columns={0: {left_on}},
                    operator=BroadcastJoinExecutor(right, small_on=right_on, big_on = left_on, suffix=suffix, how = how)
                ),
                schema=new_schema,
                ordering=None)

    def groupby(self, groupby: list, orderby=None):

        if type(groupby) == str:
            groupby = [groupby]

        assert type(groupby) == list and len(groupby) > 0, "must specify at least one group key as a list of group keys, i.e. [key1,key2]"
        if orderby is not None:
            assert type(orderby) == list 
            for i in range(len(orderby)):
                if type(orderby[i]) == tuple:
                    assert orderby[i][0] in groupby
                    assert orderby[i][1] == "asc" or orderby[i][1] == "desc"
                elif type(orderby[i]) == str:
                    assert orderby[i] in groupby
                    orderby[i] = (orderby[i],"asc")
                else:
                    raise Exception("don't understand orderby format")

        return GroupedDataStream(self, groupby=groupby, orderby=orderby)

    def _grouped_aggregate(self, groupby: list, aggregations: dict, orderby=None):
        '''
        This is a blocking operation. This will return a Dataset
        Aggregations is expected to take the form of a dictionary like this {'high':['sum'], 'low':['avg']}. The keys of this dict must be in the schema.
        The groupby argument is a list of keys to groupby. If it is empty, then no grouping is done
        '''

        # do some checks first
        for key in aggregations:
            assert (key == "*" and (aggregations[key] == ['count'] or aggregations[key] == 'count')) or (
                key in self.schema and key not in groupby)
            if type(aggregations[key]) == str:
                aggregations[key] = [aggregations[key]]
        for key in groupby:
            assert key in self.schema

        count_col = "count"

        if "*" in aggregations:
            emit_count = True
            del aggregations["*"]
            assert count_col not in self.schema, "schema conflict with column name count"
        else:
            emit_count = False

        # make all single aggregation specs into a list of one.

        # first insert the select node.
        required_columns = groupby + list(aggregations.keys())

        new_schema = groupby + [count_col + "_sum"] if len(groupby) > 0 else [count_col,count_col + "_sum"]

        '''
        We need to do two things here. The groupby-aggregation will be pushed down as a transformation.
        We need to generate the function for the transformation, as well as the necessary arguments to instantiate the AggExecutor.
        in the aggregations argument, each column could have multiple aggregations defined. However the transformation will make sure 
        that each of those aggregations gets its own column in the transformed schema, so the AggExecutor doesn't have to deal with this.
        '''
        
        pyarrow_agg_list = [(count_col, "sum")]
        agg_executor_dict = {}
        for key in aggregations:
            for agg_spec in aggregations[key]:
                if agg_spec == "avg":
                    agg_spec = "mean"
                assert agg_spec in {
                    "max", "min", "mean", "sum"}, "only support max, min, mean and sum for now"
                new_col = key + "_" + agg_spec
                assert new_col not in new_schema, "duplicate column names detected, most likely caused by a groupby column with suffix _max etc."
                new_schema.append(new_col)
                pyarrow_agg_list.append((key, agg_spec))
                agg_executor_dict[new_col] = agg_spec

        # now insert the transform node.
        # this function needs to be fast since it's executed at every batch in the actual runtime.
        def f(keys, agg_list, batch):
            enhanced_batch = batch.with_column(polars.lit(1).cast(polars.Int64).alias(count_col))
            return polars.from_arrow(enhanced_batch.to_arrow().group_by(keys).aggregate(agg_list))

        if len(groupby) > 0:
            map_func = partial(f, groupby, pyarrow_agg_list)
        else:
            map_func = partial(f, [count_col], pyarrow_agg_list)

        transformed_stream = self.transform(map_func,
                                                     new_schema=new_schema,
                                                     # it shouldn't matter too much for the required columns, since no predicates or projections will ever be pushed through this map node.
                                                     # even if this map node gets fused with prior map nodes, no predicates should ever be pushed through those map nodes either.
                                                     required_columns=set(required_columns))
        agg_node = StatefulNode(
                schema=new_schema,
                schema_mapping={col: (-1, col) for col in new_schema},
                required_columns={0: set(new_schema) },
                operator=AggExecutor(groupby if len(groupby) > 0 else [count_col], orderby, agg_executor_dict, emit_count)
            )
        agg_node.set_placement_strategy(SingleChannelStrategy())
        aggregated_stream = self.quokka_context.new_stream(
            sources={0: transformed_stream},
            partitioners={0: BroadcastPartitioner()},
            node=agg_node,
            schema=new_schema,
            ordering=None
        )

        return aggregated_stream.collect()

    def agg(self, aggregations):
        return self._grouped_aggregate([], aggregations, None)

    def aggregate(self, aggregations):
        return self.agg(aggregations)


class GroupedDataStream:
    def __init__(self, source_data_stream: DataStream, groupby, orderby) -> None:
        self.source_data_stream = source_data_stream
        self.groupby = groupby if type(groupby) == list else [groupby]
        self.orderby = orderby
        

    '''
    Similar semantics to Pandas.groupby().agg(), e.g. agg({"b":["max","min"], "c":["mean"]})
    '''

    def agg(self, aggregations: dict):
        return self.source_data_stream._grouped_aggregate(self.groupby, aggregations, self.orderby)

    '''
    Alias for agg.
    '''

    def aggregate(self, aggregations: dict):
        return self.agg(aggregations)


class DataSet:
    def __init__(self, quokka_context: QuokkaContext, schema: dict, source_node_id: int) -> None:
        self.quokka_context = quokka_context
        self.schema = schema
        self.source_node_id = source_node_id

    def to_list(self):
        return ray.get(self.wrapped_dataset.to_list.remote())

    def to_pandas(self):
        return ray.get(self.wrapped_dataset.to_pandas.remote())

    def to_dict(self):
        return ray.get(self.wrapped_dataset.to_dict.remote())
