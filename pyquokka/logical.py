import sqlglot
from pyquokka.dataset import *
from pyquokka.executors import *
from pyquokka.placement_strategy import * 
from pyquokka.utils import EC2Cluster, LocalCluster
import pyquokka.sql_utils as sql_utils
from pyquokka.target_info import * 
from functools import partial

import textwrap

def target_info_to_transform_func(target_info):

    def transform_fn(predicate, batch_funcs, projection, data):

        if data is None or len(data) == 0:
            return None
        data = data.filter(predicate)
        if len(data) == 0:
            return None
        for batch_func in batch_funcs:
            data = batch_func(data)
            if data is None or len(data) == 0:
                return None
        if projection is not None:
            data = data[sorted(list(projection))]
        return data

    def transform_fn_no_filter(batch_funcs, projection, data):

        if data is None or len(data) == 0:
            return None
        for batch_func in batch_funcs:
            data = batch_func(data)
            if data is None or len(data) == 0:
                return None
        if projection is not None:
            data = data[sorted(list(projection))]
        return data


    if target_info.predicate == sqlglot.exp.TRUE:
        return partial(transform_fn_no_filter, target_info.batch_funcs, target_info.projection)
    elif target_info.predicate == sqlglot.exp.FALSE:
        print("false predicate detected, entire subtree is useless. We should cut the entire subtree!")
        return lambda x: None
    else:    
        predicate = sql_utils.evaluate(target_info.predicate) 
        return partial(transform_fn, predicate, target_info.batch_funcs, target_info.projection)

'''
A node takes in inputs from its parent nodes, process them according to some operator, and sends outputs to targets.
A node may apply different partitioners, post-predicates and projections for different targets.
Therefore it makes no sense to talk about a "schema" for the node, since it may have different output streams, each with its own schema.
It does make sense to talk about a schema for the outputs that the node's operator will generate. 
When we refer to a node's schema, we will talk about this schema.

'''

class Node:
    def __init__(self, schema) -> None:

        self.schema = schema
        # target and partitioner information
        # target_node_id -> [Partitioner, Predicate, Projection]
        self.targets = {} 
        self.parents = {}

        self.blocking = False
        self.placement_strategy = None

        # this will be a dictionary of 
        self.output_sorted_reqs = None

        # this is a dictionary of target to estimated cardinality. All targets in self.targets must be here
        self.cardinality = {}
        self.stage = None
    
    def assign_stage(self, stage):
        self.stage = stage
    
    # this is purposefully separate from the lower pass that instantiates the operators
    # instantiating the input operators could help with the cardinality estimator,
    # but in the future, the cardinality could come from a catalog

    def set_cardinality(self):
        return None
    
    def lower(self, task_graph):
        raise NotImplementedError

    def set_output_sorted_reqs(self, reqs):
        self.output_sorted_reqs = reqs

    def set_placement_strategy(self, strategy):
        self.placement_strategy = strategy
    
    def __str__(self):
        result = str(type(self)) + '\n' + str(self.stage) + '\nParents:' + str(self.parents) + '\nTargets:' 
        for target in self.targets:
            result += "\n\t" + str(target) + " " + textwrap.fill(str(self.targets[target]))
        return result

class SourceNode(Node):
    def __init__(self, schema) -> None:
        super().__init__(schema)
        self.catalog_id = None
    
    def __str__(self):
        result = str(type(self)) + '\n' + str(self.stage) + '\nTargets:' 
        for target in self.targets:
            result += "\n\t" + str(target) + " " + str(self.targets[target])
        return result

    def set_catalog_id(self, catalog_id):
        self.catalog_id = catalog_id

    def set_cardinality(self, catalog):
        for target in self.targets:
            self.cardinality[target] = None

class InputS3FilesNode(SourceNode):
    def __init__(self, bucket, prefix, schema) -> None:
        super().__init__(schema)
        self.bucket = bucket
        self.prefix = prefix
    
    def lower(self, task_graph):
        file_reader = InputS3FilesDataset(self.bucket,self.prefix)
        node = task_graph.new_input_reader_node(file_reader, self.stage, self.placement_strategy)
        return node

class InputDiskFilesNode(SourceNode):
    def __init__(self, directory, schema) -> None:
        super().__init__(schema)
        self.directory = directory
    
    def lower(self, task_graph):
        file_reader = InputDiskFilesDataset(self.directory)
        node = task_graph.new_input_reader_node(file_reader, self.stage, self.placement_strategy)
        return node

class InputPolarsNode(SourceNode):
    def __init__(self, df) -> None:
        super().__init__(df.columns)
        self.df = df
    
    def lower(self, task_graph):
        node = task_graph.new_input_reader_node(InputPolarsDataset(self.df), self.stage, SingleChannelStrategy())
        return node

class InputRayDatasetNode(SourceNode):
    def __init__(self, dataset) -> None:
        super().__init__(dataset.schema)
        self.dataset = dataset
    
    def lower(self, task_graph):
        node = task_graph.new_input_dataset_node(self.dataset, self.stage)
        return node
    
    def set_cardinality(self, catalog):
        card = self.dataset.length()
        for target in self.targets:
            self.cardinality[target] = card

class InputS3CSVNode(SourceNode):
    def __init__(self, bucket, prefix, key, schema, sep, has_header, projection = None) -> None:
        super().__init__(schema)
        self.bucket = bucket
        self.prefix = prefix
        self.key = key
        self.sep = sep
        self.has_header = has_header
        self.projection = projection

    def lower(self, task_graph):
        csv_reader = InputS3CSVDataset(self.bucket, self.schema, prefix = self.prefix, key = self.key, sep=self.sep, header = self.has_header, stride = 16 * 1024 * 1024, columns = self.projection)
        node = task_graph.new_input_reader_node(csv_reader, self.stage, self.placement_strategy)
        return node

class InputRestGetAPINode(SourceNode):
    def __init__(self, url, arguments, headers, schema, batch_size = 10000) -> None:
        super().__init__(schema)
        self.url = url
        self.arguments = arguments
        self.batch_size = batch_size
        self.headers = headers
    
    def lower(self, task_graph):
        rest_reader = InputRestGetAPIDataset(self.url, self.arguments, self.headers, self.schema, self.batch_size)
        node = task_graph.new_input_reader_node(rest_reader, self.stage, self.placement_strategy)
        return node

class InputRestPostAPINode(SourceNode):
    def __init__(self, url, arguments, headers, schema, batch_size = 10000) -> None:
        super().__init__(schema)
        self.url = url
        self.arguments = arguments
        self.headers = headers
        self.batch_size = batch_size
    
    def lower(self, task_graph):
        rest_reader = InputRestPostAPIDataset(self.url, self.arguments, self.headers, self.schema, self.batch_size)
        node = task_graph.new_input_reader_node(rest_reader, self.stage, self.placement_strategy)
        return node

class InputDiskCSVNode(SourceNode):
    def __init__(self, filename, schema, sep, has_header, projection = None) -> None:
        super().__init__(schema)
        self.filename = filename
        self.sep = sep
        self.has_header = has_header
        self.projection = projection
    
    def set_cardinality(self, catalog):

        assert self.catalog_id is not None
        for target in self.targets:
            predicate = sql_utils.label_sample_table_names(self.targets[target].predicate)
            self.cardinality[target] = ray.get(catalog.estimate_cardinality.remote(self.catalog_id, predicate))

    def lower(self, task_graph):
        if self.output_sorted_reqs is not None:
            assert len(self.output_sorted_reqs) == 1
            key = list(self.output_sorted_reqs.keys())[0]
            val = self.output_sorted_reqs[key]
            csv_reader = InputDiskCSVDataset(self.filename, self.schema, sep=self.sep, header = self.has_header, stride = 16 * 1024 * 1024, columns = self.projection, sort_info = (key, val))
        else:
            csv_reader = InputDiskCSVDataset(self.filename, self.schema, sep=self.sep, header = self.has_header, stride = 16 * 1024 * 1024, columns = self.projection)
        node = task_graph.new_input_reader_node(csv_reader, self.stage, self.placement_strategy)
        return node

class InputS3IcebergNode(SourceNode):
    def __init__(self, table, snapshot = None, predicate = None, projection = None) -> None:
        
        schema = [i.name for i in table.schema().fields]
        super().__init__(schema)
        self.table = table # pyiceberg table format
        self.files = [task.file.file_path for task in self.table.scan(snapshot_id = snapshot).plan_files()]
        self.predicate = predicate
        self.projection = projection
        self.snapshot = snapshot

    def set_cardinality(self, catalog):     

        if self.snapshot is None:
            cardinality = self.table.metadata.snapshots[-1].summary.additional_properties['total-records']
        else:
            snapshots = self.table.metadata.snapshots
            snapshot = [i for i in snapshots if i.snapshot_id == self.snapshot][0]
            cardinality = snapshot.summary.additional_properties['total-records']

        for target in self.targets:
            self.cardinality[target] = cardinality
    
    def lower(self, task_graph):

        # if self.table.sort_orders() is not None:
        #     assert len(self.output_sorted_reqs) == 1
        #     key = list(self.output_sorted_reqs.keys())[0]
        #     val = self.output_sorted_reqs[key]
        #     parquet_reader = InputSortedEC2ParquetDataset(self.bucket, self.prefix, key, columns = list(self.projection), filters = self.predicate, mode=val)
        # else:

        parquet_reader = InputEC2ParquetDataset(bucket = None, prefix = None, files = self.files, columns = list(self.projection) if self.projection is not None else None, filters = self.predicate)
        node = task_graph.new_input_reader_node(parquet_reader, self.stage, self.placement_strategy)
        return node
    
    def __str__(self):
        result = str(type(self)) + '\nPredicate: ' + str(self.predicate) + '\nProjection: ' + str(self.projection) + '\nTargets:' 
        for target in self.targets:
            result += "\n\t" + str(target) + " " + str(self.targets[target])
        return result
    
class InputLanceNode(SourceNode):
    def __init__(self, uris, schema, vec_column, predicate = None, projection = None) -> None:
        super().__init__(schema)
        self.uris = uris
        self.predicate = predicate
        self.projection = projection
        self.vec_column = vec_column

        self.probe_df = None
        self.probe_df_col = None
        self.k = None
    
    def set_probe_df(self, probe_df, probe_df_col, k):

        assert probe_df is not None and probe_df_col is not None and k is not None, "must provide probe_df, probe_df_col, and k"
        
        assert type(probe_df) == polars.DataFrame
        self.probe_df = probe_df
        assert type(probe_df_col) == str and probe_df_col in probe_df.columns, "probe_df_col must be a string and in probe_df"
        self.probe_df_col = probe_df_col
        # self.probe_df_vecs = np.stack(self.probe_df[self.probe_df_col].to_numpy())
        assert type(k) == int and k >= 1
        self.k = k

    def set_cardinality(self, catalog):
        
        # assert self.catalog_id is not None
        for target in self.targets:
            self.cardinality[target] = None
    
    def lower(self, task_graph):
        lance_reader = InputLanceDataset(self.uris, self.vec_column, columns = list(self.projection) if self.projection is not None else None, filters = self.predicate)
        if self.probe_df is not None:
            lance_reader.set_probe_df(self.probe_df, self.probe_df_col, self.k)
        node = task_graph.new_input_reader_node(lance_reader, self.stage, self.placement_strategy)
        if self.probe_df is not None:
            operator = DFProbeDataStreamNNExecutor2(self.vec_column, self.probe_df, self.probe_df_col, self.k)
            target_info = TargetInfo(BroadcastPartitioner(), sqlglot.exp.TRUE, None, [])
            node = task_graph.new_non_blocking_node({0: node}, 
                        operator, self.stage, SingleChannelStrategy(), 
                        source_target_info={0: target_info})

        return node
        

class InputS3ParquetNode(SourceNode):
    def __init__(self, files, schema, predicate = None, projection = None) -> None:
        super().__init__(schema)
        
        self.files = files
        self.predicate = predicate
        self.projection = projection

    def set_cardinality(self, catalog):

        assert self.catalog_id is not None
        for target in self.targets:
            predicate = sql_utils.label_sample_table_names(self.targets[target].predicate)
            self.cardinality[target] = ray.get(catalog.estimate_cardinality.remote(self.catalog_id, predicate, self.predicate))
        
    
    def lower(self, task_graph):

        if self.output_sorted_reqs is not None:
            assert len(self.output_sorted_reqs) == 1
            key = list(self.output_sorted_reqs.keys())[0]
            val = self.output_sorted_reqs[key]
            parquet_reader = InputSortedEC2ParquetDataset(self.files, key, columns = list(self.projection) if self.projection is not None else None, filters = self.predicate, mode=val)
        else:
            parquet_reader = InputEC2ParquetDataset(self.files, columns = list(self.projection) if self.projection is not None else None, filters = self.predicate)
        node = task_graph.new_input_reader_node(parquet_reader, self.stage, self.placement_strategy)
        return node
    
    def __str__(self):
        result = str(type(self)) + '\nPredicate: ' + str(self.predicate) + '\nProjection: ' + str(self.projection) + '\nTargets:' 
        for target in self.targets:
            result += "\n\t" + str(target) + " " + str(self.targets[target])
        return result

class InputDiskParquetNode(SourceNode):
    def __init__(self, filepath, schema, predicate = None, projection = None) -> None:
        super().__init__(schema)
        self.filepath = filepath
        self.predicate = predicate
        self.projection = projection
    
    def set_cardinality(self, catalog):

        assert self.catalog_id is not None
        for target in self.targets:
            predicate = sql_utils.label_sample_table_names(self.targets[target].predicate)
            self.cardinality[target] = ray.get(catalog.estimate_cardinality.remote(self.catalog_id, predicate, self.predicate))

    def lower(self, task_graph):

        if type(task_graph.context.cluster) ==  EC2Cluster:
            raise Exception
        elif type(task_graph.context.cluster) == LocalCluster:
            parquet_reader = InputParquetDataset(self.filepath,  columns = list(self.projection), filters = self.predicate)
            node = task_graph.new_input_reader_node(parquet_reader, self.stage, self.placement_strategy)
            return node
    
    def __str__(self):
        result = str(type(self)) + '\nPredicate: ' + str(self.predicate) + '\nProjection: ' + str(self.projection) + '\nTargets:' 
        for target in self.targets:
            result += "\n\t" + str(target) + " " + str(self.targets[target])
        return result

class SinkNode(Node):
    def __init__(self, schema) -> None:
        super().__init__(schema)
    
    def set_cardinality(self, parent_cardinality):
        return None

class DataSetNode(SinkNode):
    def __init__(self, schema) -> None:
        super().__init__(schema)
    
    def lower(self, task_graph, parent_nodes, parent_source_info ):
        assert self.blocking
        return task_graph.new_blocking_node(parent_nodes,StorageExecutor(), self.stage, self.placement_strategy, source_target_info=parent_source_info)
        
'''
We need to keep a schema mapping to map the node's schema, aka the schema after the operator, to the schema of its parents to conduct
predicate pushdown. The schema_mapping will be a dict from column name to a tuple (i, name), where i is the index in self.parents of the parent
It can also be (-1, name), in which case the column is new.

source1 (schema)                                                                target1 (TargetInfo). required columns from predicate!
      --                                                                    --
        -- operator (schema, schema_mapping, operator required_columns)   --
      --                                                                    --
source 2 (schema)                                                               target2

operator required columns (aka required columns) is a dict, which records which columns in each source is required for computing this node

'''
class TaskNode(Node):
    def __init__(self, schema: list, schema_mapping: dict, required_columns: set) -> None:
        super().__init__(schema)
        self.schema_mapping = schema_mapping
        self.required_columns = required_columns
    
    def set_cardinality(self, parent_cardinalities):
        for target in self.targets:
            self.cardinality[target] = None
    
    def lower(self, task_graph, parent_nodes, parent_source_info ):
        raise Exception("Abstract class TaskNode cannot be lowered")

class JoinNode(TaskNode):
    def __init__(self, schema, schema_mapping, required_columns, join_spec, assume_sorted = {}) -> None:
        # you are not going to have an associated operator
        super().__init__(schema, schema_mapping, required_columns)
        self.assume_sorted = assume_sorted
        self.join_specs = [join_spec] # (join_type, {parent_idx: join_key} )

    def add_join_spec(self, join_spec):
        self.join_specs.append(join_spec)
    
    def __str__(self):
        result = 'Join Node with joins: ' + str(self.join_specs) + '\n' + str(self.stage) + '\nParents:' + str(self.parents) + '\nTargets:' 
        for target in self.targets:
            result += "\n\t" + str(target) + " " + textwrap.fill(str(self.targets[target]))
        return result

    def set_cardinality(self, parent_cardinalities):
        for target in self.targets:
            self.cardinality[target] = None

    def lower(self, task_graph, parent_nodes, parent_source_info):

        if self.blocking:
            assert len(self.targets) == 1
            target_info = self.targets[list(self.targets.keys())[0]]
            transform_func = target_info_to_transform_func(target_info)
        
        # print("lowering join node with ", len(self.join_specs), " join specs. Heuristics CBO join order used right now.")
        joined_parents = set()

        # self.join_specs = [self.join_specs[1]] + [self.join_specs[0]]

        join_type, join_info = self.join_specs[0]
        assert len(join_info) == 2
        [(left_parent, left_key), (right_parent, right_key)] = join_info

        key_to_keep = "left" if left_key in self.schema else "right"
        operator = BuildProbeJoinExecutor(None, left_key, right_key, join_type, key_to_keep)
        # operator = DiskBuildProbeJoinExecutor(None, left_key, right_key, join_type, key_to_keep)

        left_parent_target_info = parent_source_info[left_parent]
        right_parent_target_info = parent_source_info[right_parent]
        left_parent_target_info.partitioner = HashPartitioner(left_key)
        right_parent_target_info.partitioner = HashPartitioner(right_key)

        if len(self.join_specs) == 1 and self.blocking:
            return task_graph.new_blocking_node({0: parent_nodes[left_parent], 1: parent_nodes[right_parent]}, 
                        operator, self.stage, self.placement_strategy, source_target_info={0: left_parent_target_info, 1: right_parent_target_info}, transform_fn=transform_func)
        else:
            intermediate_node = task_graph.new_non_blocking_node({0: parent_nodes[left_parent], 1: parent_nodes[right_parent]},
                         operator, self.stage, self.placement_strategy, source_target_info={0: left_parent_target_info, 1: right_parent_target_info})
        joined_parents.add(parent_nodes[left_parent])
        joined_parents.add(parent_nodes[right_parent])

        for i in range(1, len(self.join_specs)):
            join_type, join_info = self.join_specs[i]
            assert len(join_info) == 2
            [(left_parent, left_key), (right_parent, right_key)] = join_info

            # left parent is always the probe. right parent is always the build
            key_to_keep = "left" if left_key in self.schema else "right"
            operator = BuildProbeJoinExecutor(None, left_key, right_key, join_type, key_to_keep)
            # operator = DiskBuildProbeJoinExecutor(None, left_key, right_key, join_type, key_to_keep)

            assert  parent_nodes[left_parent] in joined_parents
            intermediate_target_info = TargetInfo(HashPartitioner(left_key), sqlglot.exp.TRUE, None, [])
            parent_target_info = parent_source_info[right_parent]
            parent_target_info.partitioner = HashPartitioner(right_key)
            # print("adding node", intermediate_node, parent_nodes[right], {0: str(intermediate_target_info), 1: str(parent_target_info)})
            if i == len(self.join_specs) - 1 and self.blocking:
                return task_graph.new_blocking_node({0: intermediate_node, 1: parent_nodes[right_parent]}, 
                        operator, self.stage, self.placement_strategy, source_target_info={0: intermediate_target_info, 1: parent_target_info}, transform_fn=transform_func)
            else:
                intermediate_node = task_graph.new_non_blocking_node({0: intermediate_node, 1: parent_nodes[right_parent]}, 
                        operator, self.stage, self.placement_strategy, source_target_info={0: intermediate_target_info, 1: parent_target_info})
            joined_parents.add(parent_nodes[right_parent])
        
        return intermediate_node

class BroadcastJoinNode(TaskNode):
    def __init__(self, schema, schema_mapping, required_columns, operator, assume_sorted = {}) -> None:
        """
        Args:
            schema: the schema after the operator
            schema_mapping: a dict from column name to a tuple (i, name), where i is the index in self.parents of the parent
            assume_sorted: a dict from source index to True or False, indicating whether the node expects if the source is sorted
        """
        super().__init__(schema, schema_mapping, required_columns)
        self.operator = operator
        self.assume_sorted = assume_sorted
    
    def set_cardinality(self, parent_cardinalities):
        assert len(parent_cardinalities) == 1
        parent_cardinality = list(parent_cardinalities.values())[0]
        for target in self.targets:
            if self.targets[target].predicate == sqlglot.exp.TRUE:
                self.cardinality[target] = parent_cardinality
            else:
                self.cardinality[target] = None
    
    def lower(self, task_graph, parent_nodes, parent_source_info ):
        
        if self.blocking:
            assert len(self.targets) == 1
            target_info = self.targets[list(self.targets.keys())[0]]
            transform_func = target_info_to_transform_func(target_info)          
            
            return task_graph.new_blocking_node(parent_nodes,self.operator, self.stage, self.placement_strategy, source_target_info=parent_source_info, transform_fn = transform_func, assume_sorted = self.assume_sorted)
        else:
            return task_graph.new_non_blocking_node(parent_nodes,self.operator, self.stage, self.placement_strategy, source_target_info=parent_source_info, assume_sorted = self.assume_sorted)


class StatefulNode(TaskNode):
    def __init__(self, schema, schema_mapping, required_columns, operator, assume_sorted = {}) -> None:
        """
        Args:
            schema: the schema after the operator
            schema_mapping: a dict from column name to a tuple (i, name), where i is the index in self.parents of the parent
            assume_sorted: a dict from source index to True or False, indicating whether the node expects if the source is sorted
        """
        super().__init__(schema, schema_mapping, required_columns)
        self.operator = operator
        self.assume_sorted = assume_sorted
    
    def lower(self, task_graph, parent_nodes, parent_source_info ):
        
        if self.blocking:
            assert len(self.targets) == 1
            target_info = self.targets[list(self.targets.keys())[0]]
            transform_func = target_info_to_transform_func(target_info)          
            
            return task_graph.new_blocking_node(parent_nodes,self.operator, self.stage, self.placement_strategy, source_target_info=parent_source_info, transform_fn = transform_func, assume_sorted = self.assume_sorted)
        else:
            return task_graph.new_non_blocking_node(parent_nodes,self.operator, self.stage, self.placement_strategy, source_target_info=parent_source_info, assume_sorted = self.assume_sorted)

'''
We need a separate MapNode from StatefulNode since we can compact UDFs

foldable controls if this MapNode should be fused upstream. 

You might not want to fold if 
- you want to dynamically batch the UDF, i.e. if the UDF is a lot more efficient when the batch size is larger
- you want parallelized fault recovery for this UDF, i.e. computing this UDF is very expensive
- you want the backend to potentially spawn more/different resources for this UDF, i.e. computing this UDF is very expensive.

You definitely want to fold if
- applying this UDF greatly saves communication costs, i.e. pre-aggregation
- UDF is simple and fast. This avoids memory copying to the object store.

There are two kinds of map nodes. One that appends new columns to a record batch while maintaining its length, another that 
transforms an entire record batch. Both can be described by this class. 
'''
class MapNode(TaskNode):
    def __init__(self, schema, schema_mapping, required_columns, function, foldable = True) -> None:
        super().__init__(schema, schema_mapping, required_columns)
        self.function = function
        self.foldable = foldable

    def lower(self, task_graph, parent_nodes, parent_source_info ):
        if self.blocking:
            assert len(self.targets) == 1
            target_info = self.targets[list(self.targets.keys())[0]]
            transform_func = target_info_to_transform_func(target_info)     

            return task_graph.new_blocking_node(parent_nodes,UDFExecutor(self.function), self.stage, self.placement_strategy, source_target_info=parent_source_info, transform_fn = transform_func)
        else:
            return task_graph.new_non_blocking_node(parent_nodes,UDFExecutor(self.function), self.stage, self.placement_strategy, source_target_info=parent_source_info)

class FilterNode(TaskNode):

    # predicate must be CNF
    def __init__(self, schema, predicate: sqlglot.Expression) -> None:
        super().__init__(
            schema = schema, 
            schema_mapping = {column: {0: column} for column in schema}, 
            required_columns = {0:set(i.name for i in predicate.find_all(sqlglot.expressions.Column))})
        self.predicate = predicate
    
    def __str__(self):
        result = "filter node: " + self.predicate.sql(dialect = "duckdb")
        return result
    
    def lower(self, task_graph, parent_nodes, parent_source_info):
        print("Tried to lower a filter node. This means the optimization probably failed and something bad is happening.")
        raise NotImplementedError

class NearestNeighborFilterNode(TaskNode):

    def __init__(self, schema: list, schema_mapping: dict, vec_column: str, probe_df, probe_vector_col: str, k: int) -> None:
        super().__init__(
            schema = schema,
            schema_mapping = schema_mapping,
            required_columns = {0: {vec_column}})
        self.vec_column = vec_column
        self.probe_df = probe_df
        self.k = k
        self.probe_vector_col = probe_vector_col
    
    def __str__(self):
        result = "nearest neighbor filter node on vec column {} with {} probe vectors".format(self.vec_column, len(self.probe_df))
        return result
    
    def set_cardinality(self, parent_cardinalities):
        assert len(parent_cardinalities) == 1
        parent_cardinality = list(parent_cardinalities.values())[0]
        for target in self.targets:
            if self.targets[target].predicate == sqlglot.exp.TRUE and parent_cardinality is not None:
                self.cardinality[target] = parent_cardinality * self.k
            else:
                self.cardinality[target] = None
    
    def lower(self, task_graph, parent_nodes, parent_source_info):

        executor1 = DFProbeDataStreamNNExecutor1(self.vec_column, self.probe_df, self.probe_vector_col, self.k)
        executor2 = DFProbeDataStreamNNExecutor2(self.vec_column, self.probe_df, self.probe_vector_col, self.k)

        intermediate_target_info = TargetInfo(BroadcastPartitioner(), sqlglot.exp.TRUE, None, [])

        if self.blocking:
            assert len(self.targets) == 1
            target_info = self.targets[list(self.targets.keys())[0]]
            transform_func = target_info_to_transform_func(target_info)     
            intermediates = task_graph.new_non_blocking_node(parent_nodes,executor1, self.stage, self.placement_strategy, source_target_info=parent_source_info)
            return task_graph.new_blocking_node({0: intermediates}, executor2, self.stage, SingleChannelStrategy(), source_target_info={0: intermediate_target_info}, transform_fn = transform_func)

        else:
            intermediates = task_graph.new_non_blocking_node(parent_nodes,executor1, self.stage, self.placement_strategy, source_target_info=parent_source_info)
            return task_graph.new_non_blocking_node({0: intermediates}, executor2, self.stage, SingleChannelStrategy(), source_target_info={0: intermediate_target_info})

class ProjectionNode(TaskNode):

    # predicate must be CNF
    def __init__(self, projection: set) -> None:
        super().__init__(
            schema = projection, 
            schema_mapping = {column: {0: column} for column in projection}, 
            required_columns = {0:projection})
        self.projection = projection
    
    def __str__(self):
        result = str("projection node: ") + str(self.projection)
        return result
