from pyquokka.datastream import * 

class OrderedStream(DataStream):
    def __init__(self, datastream, sorted_reqs) -> None:
        super().__init__(datastream.quokka_context, datastream.schema, datastream.source_node_id, sorted_reqs, datastream.materialized)
    
    def __str__(self):
        return "OrderedStream[" + ",".join(self.schema) + "] order by {}".format(self.sorted)

    def __repr__(self):
        return "OrderedStream[" + ",".join(self.schema) + "] order by {}".format(self.sorted)

    def shift(self, n, by = None, fill_value=None):
        """
        Shifts the elements of this stream by `n` positions, inserting `fill_value`
        for the new elements created at the beginning or end of the stream.

        :param n: the number of positions to shift the elements (positive or negative) positive means shift forward
        :param fill_value: the value to use for the new elements created at the beginning
                        or end of the stream (default: None)
        :return: a new stream with the shifted elements
        """

        return self.quokka_context.new_stream(
            sources={0: self},
            partitioners={0: HashPartitioner()},
            node=StatefulNode(
                schema=self.schema,
                schema_mapping={col: {0: col} for col in self.schema},
                required_columns={0: set(self.schema)},
                operator=ShiftOperator(n, fill_value),
                assume_sorted={0:True},
            ),
            schema=self.schema,
        )
    
    def pattern_recognize(self, time_col, events, maxspan, by):
        
        # we are going to use the cer method from the cep_utils module
        if by is None:
            raise Exception("You must specify a key by column for pattern_recognize currently.")
        
        assert time_col in self.sorted
        assert by in self.schema

        executor = CEPExecutor(events, maxspan, time_col, by)
        if executor.prefilter != sqlglot.exp.FALSE:
            filtered_stream = self.filter_sql(executor.prefilter)
        else:
            filtered_stream = self
        
        return filtered_stream.stateful_transform(executor, ["event_number", "first_event_timestamp", "last_event_timestamp"],
                                                        required_columns = executor.touched_columns.union({time_col, by}), by = by, placement_strategy = CustomChannelsStrategy(1))

    def stateful_transform(self, executor: Executor, new_schema: list, required_columns: set,
                           by = None, placement_strategy = CustomChannelsStrategy(1)):

        """

        **EXPERIMENTAL API** 

        This is like `transform`, except you can use a stateful object as your transformation function. This is useful for example, if you want to run
        a heavy Pytorch model on each batch coming in, and you don't want to reload this model for each function call. Remember the `transform` API only
        supports stateless transformations. You could also implement much more complicated stateful transformations, like implementing your own aggregation
        function if you are not satisfied with Quokka's default operator's performance.

        This API is still being finalized. A version of it that takes multiple input streams is also going to be added. This is the part of the DataStream level 
        api that is closest to the underlying execution engine. Quokka's underlying execution engine basically executes a series of stateful transformations
        on batches of data. The difficulty here is how much of that underlying API to expose here so it's still useful without the user having to understand 
        how the Quokka runtime works. To that end, we have to come up with suitable partitioner and placement strategy abstraction classes and interfaces.

        If you are interested in helping us hammer out this API, please talke to me: zihengw@stanford.edu.

        Args:
            executor (pyquokka.executors.Executor): The stateful executor. It must be a subclass of `pyquokka.executors.Executor`, and expose the `execute` 
                and `done` functions. More details forthcoming.
            new_schema (list): The names of the columns of the Polars DataFrame that the transformation function produces. 
            required_columns (list or set): The names of the columns that are required for this transformation. This argument is made mandatory
                because it's often trivial to supply and can often greatly speed things up.

        Return:
            A transformed DataStream.
        
        Examples:
            Check the code for the `gramian` function.
        """
        if type(required_columns) == list:
            required_columns = set(required_columns)
        assert type(required_columns) == set
        assert issubclass(type(executor), Executor), "user defined executor must be an instance of a \
            child class of the Executor class defined in pyquokka.executors. You must override the execute and done methods."

        select_stream = self.select(required_columns)

        custom_node = StatefulNode(
            schema=new_schema,
            # cannot push through any predicates or projections!
            schema_mapping={col: {-1: col} for col in new_schema},
            required_columns={0: required_columns},
            operator=executor,
            assume_sorted={0:True}
        )

        custom_node.set_placement_strategy(placement_strategy)

        return self.quokka_context.new_stream(
            sources={0: select_stream},
            partitioners={0: PassThroughPartitioner() if by is None else HashPartitioner(by)},
            node=custom_node,
            schema=new_schema,
            
        )

    def join_asof(self, right, on=None, left_on=None, right_on=None, by=None, left_by = None, right_by = None, suffix="_2"):

        assert type(right) == OrderedStream
        if on is not None:
            assert left_on is None and right_on is None
            left_on = on
            right_on = on
        else:
            assert left_on is not None and right_on is not None
        
        assert left_on in self.schema
        assert right_on in right.schema
        
        if by is None and left_by is None and right_by is None:
            raise Exception("Currently non-grouped asof join is not supported. Please raise Github issue")
        
        if by is not None:
            assert left_by is None and right_by is None
            left_by = by
            right_by = by
        else:
            assert left_by is not None and right_by is not None
        
        assert left_by in self.schema
        assert right_by in right.schema

        new_schema = self.schema.copy()
        if self.materialized:
            schema_mapping = {col: {-1: col} for col in self.schema}
        else:
            schema_mapping = {col: {0: col} for col in self.schema}

        if right.materialized:
            right_table_id = -1
        else:
            right_table_id = 1

        rename_dict = {}

        # import pdb;pdb.set_trace()

        right_cols = right.schema
        for col in right_cols:
            if col == right_on or col == right_by:
                continue
            if col in new_schema:
                assert col + \
                    suffix not in new_schema, (
                        "the suffix was not enough to guarantee unique col names", col + suffix, new_schema)
                new_schema.append(col + suffix)
                schema_mapping[col+suffix] = {right_table_id: col + suffix}
                rename_dict[col] = col + suffix
            else:
                new_schema.append(col)
                schema_mapping[col] = {right_table_id: col}
        
        if len(rename_dict) > 0:
            right = right.rename(rename_dict)


        executor = SortedAsofExecutor(left_on, right_on, left_by, right_by, suffix)

        return self.quokka_context.new_stream(
                sources={0: self, 1: right},
                partitioners={0: HashPartitioner(
                    left_by), 1: HashPartitioner(right_by)},
                node=StatefulNode(
                    schema=new_schema,
                    schema_mapping=schema_mapping,
                    required_columns={0: {left_on, left_by}, 1: {right_on, right_by}},
                    operator= executor,
                    assume_sorted={0:True, 1: True}),
                schema=new_schema,
                )
        