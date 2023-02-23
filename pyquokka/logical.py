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
    
    def __str__(self):
        result = str(type(self)) + '\n' + str(self.stage) + '\nTargets:' 
        for target in self.targets:
            result += "\n\t" + str(target) + " " + str(self.targets[target])
        return result

    def set_cardinality(self):
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
    def __init__(self, df, schema) -> None:
        super().__init__(schema)
        self.df = df
    
    def lower(self, task_graph):
        node = task_graph.new_input_reader_node(self.df, self.stage, SingleChannelStrategy())
        return node

class InputRayDatasetNode(SourceNode):
    def __init__(self, dataset) -> None:
        super().__init__(dataset.schema)
        self.dataset = dataset
    
    def lower(self, task_graph):
        node = task_graph.new_input_dataset_node(self.dataset, self.stage)
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
    
    def set_cardinality(self):
        if os.path.isfile(self.filename):
            files = [self.filename]
            sizes = [os.path.getsize(self.filename)]
        else:
            assert os.path.isdir(self.filename), "Does not support prefix, must give absolute directory path for a list of files, will read everything in there!"
            files = [self.filename + "/" + file for file in os.listdir(self.filename)]
            sizes = [os.path.getsize(file) for file in files]
        
        # let's now sample from one file. This can change in the future
        # we will sample 10 MB from the file

        file_to_do = np.random.choice(files, p = np.array(sizes) / sum(sizes))
        start_pos = np.random.randint(0, max(os.path.getsize(file_to_do) - 10 * 1024 * 1024,1))
        with open(file_to_do, 'rb') as f:
            f.seek(start_pos)
            sample = f.read(10 * 1024 * 1024)
        first_new_line = sample.find(b'\n')
        last_new_line = sample.rfind(b'\n')
        sample = sample[first_new_line + 1 : last_new_line]

        # now read sample as a csv
        sample = csv.read_csv(BytesIO(sample), read_options=csv.ReadOptions(
                column_names=self.schema), parse_options=csv.ParseOptions(delimiter=self.sep))

        # now apply the predicate to this sample
        for target in self.targets:
            predicate = sql_utils.label_sample_table_names(self.targets[target].predicate)
            if predicate == sqlglot.exp.TRUE:
                count = len(sample)
            else:
                sql_statement = "select count(*) from sample where " + predicate.sql()
                con = duckdb.connect().execute('PRAGMA threads=%d' % 8)
                count = con.execute(sql_statement).fetchall()[0][0]
            estimated_cardinality = count * sum(sizes) / (10 * 1024 * 1024)
            self.cardinality[target] = estimated_cardinality

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

    def set_cardinality(self):     

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

class InputS3ParquetNode(SourceNode):
    def __init__(self, bucket, prefix, key, schema, predicate = None, projection = None) -> None:
        super().__init__(schema)
        
        self.prefix = prefix
        self.key = key
        self.bucket = bucket
        self.predicate = predicate
        self.projection = projection

        assert bucket is not None
        assert (prefix is None) != (key is None) # xor

    def set_cardinality(self):
        s3fs = S3FileSystem()
        if self.key is not None:
            dataset = pq.ParquetDataset(self.bucket + "/" + self.prefix, filesystem=s3fs )
        else:
            dataset = pq.ParquetDataset(self.bucket + "/" + self.key, filesystem=s3fs )
        # very cursory estimate
        size = dataset.fragments[0].count_rows() * len(dataset.fragments)
        for target in self.targets:
            self.cardinality[target] = size
    
    def lower(self, task_graph):

        if self.key is None:
            if self.output_sorted_reqs is not None:
                assert len(self.output_sorted_reqs) == 1
                key = list(self.output_sorted_reqs.keys())[0]
                val = self.output_sorted_reqs[key]
                parquet_reader = InputSortedEC2ParquetDataset(self.bucket, self.prefix, key, columns = list(self.projection) if self.projection is not None else None, filters = self.predicate, mode=val)
            else:
                parquet_reader = InputEC2ParquetDataset(bucket = self.bucket, prefix = self.prefix, columns = list(self.projection) if self.projection is not None else None, filters = self.predicate)
        else:
            parquet_reader = InputParquetDataset(self.bucket + "/" + self.key, mode = "s3", columns = list(self.projection) if self.projection is not None else None, filters = self.predicate)
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
        
        print("lowering join node with ", len(self.join_specs), " join specs. Random join order used right now.")
        joined_parents = set()

        # self.join_specs = [self.join_specs[1]] + [self.join_specs[0]]

        join_type, join_info = self.join_specs[0]
        assert len(join_info) == 2
        [(left_parent, left_key), (right_parent, right_key)] = join_info

        key_to_keep = "left" if left_key in self.schema else "right"
        operator = BuildProbeJoinExecutor(None, left_key, right_key, join_type, key_to_keep)

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
            schema_mapping = {column: (0, column) for column in schema}, 
            required_columns = {0:set(i.name for i in predicate.find_all(sqlglot.expressions.Column))})
        self.predicate = predicate
    
    def __str__(self):
        result = "filter node: " + self.predicate.sql()
        return result
    
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
