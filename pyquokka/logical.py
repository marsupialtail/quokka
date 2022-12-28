import sqlglot
from pyquokka.dataset import *
from pyquokka.executors import StorageExecutor, UDFExecutor 
from pyquokka.utils import EC2Cluster, LocalCluster
import pyquokka.sql_utils as sql_utils
from functools import partial

import textwrap

def target_info_to_transform_func(target_info):

    def transform_fn(predicate, batch_funcs, projection, data):

        if data is None or len(data) == 0:
            return None
        if predicate != True:
            data = data.filter(predicate(data))
        if len(data) == 0:
            return None
        for batch_func in batch_funcs:
            data = batch_func(data)
            if data is None or len(data) == 0:
                return None
        if projection is not None:
            data = data[sorted(list(projection))]
        return data


    if target_info.predicate == sqlglot.exp.TRUE:
        predicate = True
    elif target_info.predicate == False:
        print("false predicate detected, entire subtree is useless. We should cut the entire subtree!")
        predicate = sql_utils.evaluate(target_info.predicate) 
    else:    
       predicate = sql_utils.evaluate(target_info.predicate) 
    
    return partial(transform_fn, predicate, target_info.batch_funcs, target_info.projection)


class PlacementStrategy:
    def __init__(self) -> None:
        pass

'''
Launch a single channel. Useful for aggregator nodes etc.
'''
class SingleChannelStrategy(PlacementStrategy):
    def __init__(self) -> None:
        super().__init__()

'''
Launch a custom number of cahnnels per node. Note the default is cpu_count for SourceNodes and 1 for task nodes.
'''
class CustomChannelsStrategy(PlacementStrategy):
    def __init__(self, channels) -> None:
        super().__init__()
        self.channels_per_node = channels

'''
Launch only on GPU instances.
'''
class GPUStrategy(PlacementStrategy):
    def __init__(self) -> None:
        super().__init__()

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
    
    def lower(self, task_graph):
        raise NotImplementedError
    
    def set_placement_strategy(self, strategy):
        self.placement_strategy = strategy
    
    def __str__(self):
        result = str(type(self)) + '\nParents:' + str(self.parents) + '\nTargets:' 
        for target in self.targets:
            result += "\n\t" + str(target) + " " + textwrap.fill(str(self.targets[target]))
        return result

class SourceNode(Node):
    def __init__(self, schema) -> None:
        super().__init__(schema)
    
    def __str__(self):
        result = str(type(self)) + '\nTargets:' 
        for target in self.targets:
            result += "\n\t" + str(target) + " " + str(self.targets[target])
        return result

class InputS3FilesNode(SourceNode):
    def __init__(self, bucket, prefix, schema) -> None:
        super().__init__(schema)
        self.bucket = bucket
        self.prefix = prefix
    
    def lower(self, task_graph):
        file_reader = InputS3FilesDataset(self.bucket,self.prefix)
        node = task_graph.new_input_reader_node(file_reader, self.placement_strategy)
        return node

class InputDiskFilesNode(SourceNode):
    def __init__(self, directory, schema) -> None:
        super().__init__(schema)
        self.directory = directory
    
    def lower(self, task_graph):
        file_reader = InputDiskFilesDataset(self.directory)
        node = task_graph.new_input_reader_node(file_reader, self.placement_strategy)
        return node

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
        node = task_graph.new_input_reader_node(csv_reader, self.placement_strategy)
        return node

class InputDiskCSVNode(SourceNode):
    def __init__(self, filename, schema, sep, has_header, projection = None) -> None:
        super().__init__(schema)
        self.filename = filename
        self.sep = sep
        self.has_header = has_header
        self.projection = projection

    def lower(self, task_graph):
        csv_reader = InputDiskCSVDataset(self.filename, self.schema, sep=self.sep, header = self.has_header, stride = 16 * 1024 * 1024, columns = self.projection)
        node = task_graph.new_input_reader_node(csv_reader, self.placement_strategy)
        return node

class InputS3ParquetNode(SourceNode):
    def __init__(self, bucket, prefix, key, schema, predicate = None, projection = None) -> None:
        super().__init__(schema)
        assert (prefix is None) != (key is None) # xor
        self.prefix = prefix
        self.key = key
        self.bucket = bucket
        self.predicate = predicate
        self.projection = projection
    
    def lower(self, task_graph):

        if type(task_graph.cluster) ==  EC2Cluster:
            if self.key is None:
                parquet_reader = InputEC2ParquetDataset(self.bucket, self.prefix, columns = list(self.projection), filters = self.predicate)
            else:
                parquet_reader = InputParquetDataset(self.bucket + "/" + self.key, mode = "s3", columns = list(self.projection), filters = self.predicate)
            node = task_graph.new_input_reader_node(parquet_reader, self.placement_strategy)
            return node
        elif type(task_graph.cluster) == LocalCluster:
            if self.key is None:
                parquet_reader = InputEC2ParquetDataset(self.bucket, self.prefix, columns = list(self.projection), filters = self.predicate)
            else:
                parquet_reader = InputParquetDataset(self.bucket + "/" + self.key, mode = "s3", columns = list(self.projection), filters = self.predicate)
            node = task_graph.new_input_reader_node(parquet_reader, self.placement_strategy)
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

    def lower(self, task_graph):

        if type(task_graph.cluster) ==  EC2Cluster:
            raise Exception
        elif type(task_graph.cluster) == LocalCluster:
            parquet_reader = InputParquetDataset(self.filepath,  columns = list(self.projection), filters = self.predicate)
            node = task_graph.new_input_reader_node(parquet_reader, self.placement_strategy)
            return node
    
    def __str__(self):
        result = str(type(self)) + '\nPredicate: ' + str(self.predicate) + '\nProjection: ' + str(self.projection) + '\nTargets:' 
        for target in self.targets:
            result += "\n\t" + str(target) + " " + str(self.targets[target])
        return result

class SinkNode(Node):
    def __init__(self, schema) -> None:
        super().__init__(schema)

class DataSetNode(SinkNode):
    def __init__(self, schema) -> None:
        super().__init__(schema)
    
    def lower(self, task_graph, parent_nodes, parent_source_info ):
        assert self.blocking
        return task_graph.new_blocking_node(parent_nodes,StorageExecutor(), self.placement_strategy, source_target_info=parent_source_info)
        
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
        

class StatefulNode(TaskNode):
    def __init__(self, schema, schema_mapping, required_columns, operator) -> None:
        super().__init__(schema, schema_mapping, required_columns)
        self.operator = operator
    
    def lower(self, task_graph, parent_nodes, parent_source_info ):
        if self.blocking:
            assert len(self.targets) == 1
            target_info = self.targets[list(self.targets.keys())[0]]
            transform_func = target_info_to_transform_func(target_info)          
            
            return task_graph.new_blocking_node(parent_nodes,self.operator, self.placement_strategy, source_target_info=parent_source_info, transform_fn = transform_func)
        else:
            return task_graph.new_non_blocking_node(parent_nodes,self.operator, self.placement_strategy, source_target_info=parent_source_info)
        
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

            return task_graph.new_blocking_node(parent_nodes,UDFExecutor(self.function), self.placement_strategy, source_target_info=parent_source_info, transform_fn = transform_func)
        else:
            return task_graph.new_non_blocking_node(parent_nodes,UDFExecutor(self.function), self.placement_strategy, source_target_info=parent_source_info)

class FilterNode(TaskNode):

    # predicate must be CNF
    def __init__(self, schema, predicate: sqlglot.Expression) -> None:
        super().__init__(
            schema = schema, 
            schema_mapping = {column: (0, column) for column in schema}, 
            required_columns = {0:set(i.name for i in predicate.find_all(sqlglot.expressions.Column))})
        self.predicate = predicate
    
    def lower(self, task_graph, parent_nodes, parent_source_info):
        print("Tried to lower a filter node. This means the optimization probably failed and something bad is happening.")
        raise NotImplementedError

class ProjectionNode(TaskNode):

    # predicate must be CNF
    def __init__(self, projection: set) -> None:
        super().__init__(
            schema = projection, 
            schema_mapping = {column: (0, column) for column in projection}, 
            required_columns = {0:projection})
        self.projection = projection
    
    def __str__(self):
        result = str("projection node: ") + str(self.projection)
        return result