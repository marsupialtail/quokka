from os import rename
from pyquokka.executors import *
from pyquokka.dataset import *
from pyquokka.logical import *
from pyquokka.target_info import *
from pyquokka.quokka_runtime import * 
from pyquokka.utils import EC2Cluster, LocalCluster
from functools import partial
import pyarrow as pa

class DataStream:
    def __init__(self, quokka_context, schema: list, source_node_id: int) -> None:
        self.quokka_context = quokka_context
        self.schema = schema
        self.source_node_id = source_node_id

    def __str__(self):
        return "DataStream[" +",".join(self.schema) + "]"
    
    def __repr__(self):
        return "DataStream[" +",".join(self.schema) + "]"

    def collect(self):
        """
        This will trigger the execution of computational graph, similar to Spark collect
        The result will be a Polars DataFrame on the master

        Return:
            Dataset.
        """
        dataset = self.quokka_context.new_dataset(self, self.schema)
        return self.quokka_context.execute_node(dataset.source_node_id)

    def compute(self):
        """
        This will trigger the execution of computational graph, similar to Spark collect
        The result will be a Dataset, which you can then call to_df() or call to_stream() to initiate another computation.
        """
        dataset = self.quokka_context.new_dataset(self, self.schema)
        return self.quokka_context.execute_node(dataset.source_node_id, collect=False)
    
    def explain(self, mode="graph"):
        '''
        This will not trigger the execution of your computation graph but will produce a graph of the execution plan.
        '''
        dataset = self.quokka_context.new_dataset(self, self.schema)
        return self.quokka_context.execute_node(dataset.source_node_id, explain = True, mode= mode)

    def write_csv(self, table_location , output_line_limit = 1000000):

        if table_location[:5] == "s3://":

            if type(self.quokka_context.cluster) == LocalCluster:
                print("Warning: trying to write S3 dataset on local machine. This assumes high network bandwidth.")

            table_location = table_location[5:]
            executor = OutputExecutor(table_location, "csv", mode="s3", row_group_size=output_line_limit)

        else:

            if type(self.quokka_context.cluster) == EC2Cluster:
                raise NotImplementedError("Does not support wQuokkariting local dataset with S3 cluster. Must use S3 bucket.")

            assert table_location[0] == "/", "You must supply absolute path to directory."
            assert os.path.isdir(table_location), "Must supply an existing directory"
        
            executor = OutputExecutor(table_location, "csv", mode = "local", row_group_size= output_line_limit)

        name_stream = self.quokka_context.new_stream(
            sources={0: self},
            partitioners={0: PassThroughPartitioner()},
            node=StatefulNode(
                schema=["filename"],
                # this is a stateful node, but predicates and projections can be pushed down.
                schema_mapping={"filename":(-1, "filename")},
                required_columns={0: set(self.schema)},
                operator=executor
            ),
            schema=["filename"],
            ordering=None
        )

        return name_stream.collect()

    def write_parquet(self, table_location, output_line_limit= 10000000):
        if table_location[:5] == "s3://":

            if type(self.quokka_context.cluster) == LocalCluster:
                print("Warning: trying to write S3 dataset on local machine. This assumes high network bandwidth.")

            table_location = table_location[5:]
            executor = OutputExecutor(table_location, "parquet", mode="s3", row_group_size=output_line_limit)

        else:

            if type(self.quokka_context.cluster) == EC2Cluster:
                raise NotImplementedError("Does not support writing local dataset with S3 cluster. Must use S3 bucket.")

            assert table_location[0] == "/", "You must supply absolute path to directory."
        
            executor = OutputExecutor(table_location, "parquet", mode = "local", row_group_size= output_line_limit)

        name_stream = self.quokka_context.new_stream(
            sources={0: self},
            partitioners={0: PassThroughPartitioner()},
            node=StatefulNode(
                schema=["filename"],
                # this is a stateful node, but predicates and projections can be pushed down.
                schema_mapping={"filename":(-1, "filename")},
                required_columns={0: set(self.schema)},
                operator=executor
            ),
            schema=["filename"],
            ordering=None
        )

        return name_stream.collect()

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
    
    def drop(self, cols_to_drop: list):

        for col in cols_to_drop:
            assert col in self.schema, "col to drop not found in schema"
        return self.select([col for col in self.schema if col not in cols_to_drop])

    def rename(self, rename_dict):

        assert type(rename_dict) == dict, "must specify a dictionary like Polars"
        for key in rename_dict:
            assert key in self.schema, "key in rename dict must be in schema"
            assert rename_dict[key] not in self.schema, "new name must not be in current schema"
        
        # the fact you can write this in one line is why I love Python
        new_schema = [col if col not in rename_dict else rename_dict[col] for col in self.schema ]
        schema_mapping = {}
        for key in rename_dict:
            schema_mapping[rename_dict[key]] = (0, key)
        for key in self.schema:
            if key not in rename_dict:
                schema_mapping[key] = (0, key)

        f = lambda x: x.rename(rename_dict)

        return self.quokka_context.new_stream(
            sources={0: self},
            partitioners={0: PassThroughPartitioner()},
            node=MapNode(
                schema=new_schema,
                schema_mapping=schema_mapping,
                required_columns={0: set(rename_dict.keys())},
                function=f,
                foldable=True
            ),
            schema=new_schema,
            ordering=None
        )


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


    def stateful_transform(self, executor: Executor, new_schema: list, required_columns: set, 
        partitioner = PassThroughPartitioner() ,placement = "cpu"):

        assert type(required_columns) == set
        assert issubclass(type(executor), Executor), "user defined executor must be an instance of a \
            child class of the Executor class defined in pyquokka.executors. You must override the execute and done methods."

        select_stream = self.select(required_columns)

        custom_node = StatefulNode(
                schema=new_schema,
                # cannot push through any predicates or projections!
                schema_mapping={col: (-1, col) for col in new_schema},
                required_columns={0: required_columns},
                operator=executor
            )
        
        assert placement == "cpu" or placement == "gpu"
        if placement == "gpu":
            raise NotImplementedError("does not support GPU yet! Please email me zihengw@stanford.edu")

        return self.quokka_context.new_stream(
            sources={0: select_stream},
            partitioners={0: partitioner},
            node = custom_node,
            schema=new_schema,
            ordering=None
        )

    '''
    This is an optimization where we allow a groupby to be done in a nonblocking fashion if all we want is the keys
    If you want actual aggregations, then it's no longer possible to do it nonblockiing
    '''
    def distinct(self, keys: list):

        if type(keys) == str:
            keys = [keys]

        for key in keys:
            assert key in self.schema

        select_stream = self.select(keys)

        return self.quokka_context.new_stream(
            sources={0: select_stream},
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

        assert type(right) == polars.internals.DataFrame or issubclass(type(right), DataStream)

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
        if type(right) == polars.internals.DataFrame:
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
        elif type(right) == polars.internals.DataFrame:
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
            for i in range(len(aggregations[key])):
                if aggregations[key][i] == "avg":
                    aggregations[key][i] = "mean"
                    assert aggregations[key][i] in {
                        "max", "min", "mean", "sum"}, "only support max, min, mean and sum for now"
        for key in groupby:
            assert key in self.schema

        count_col = "__count"

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
        # key is column name, value is Boolean to indicate if you should keep around the sum column.
        mean_cols = {}
        for key in aggregations:
            for agg_spec in aggregations[key]:
                if agg_spec == "mean":

                    if "sum" in aggregations[key]:
                        mean_cols[key] = True
                    
                    else:
                        new_col = key + "_sum"
                        assert new_col not in new_schema, "duplicate column names detected, most likely caused by a groupby column with suffix _max etc."
                        new_schema.append(new_col)
                        pyarrow_agg_list.append((key, "sum"))
                        agg_executor_dict[new_col] = "sum"
                        mean_cols[key] = False
                
                else:
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
                operator=AggExecutor(groupby if len(groupby) > 0 else [count_col], orderby, agg_executor_dict, mean_cols, emit_count)
            )
        agg_node.set_placement_strategy(SingleChannelStrategy())
        aggregated_stream = self.quokka_context.new_stream(
            sources={0: transformed_stream},
            partitioners={0: BroadcastPartitioner()},
            node=agg_node,
            schema=new_schema,
            ordering=None
        )

        return aggregated_stream

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