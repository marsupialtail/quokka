from pyquokka.datastream import * 

class OrderedStream(DataStream):
    def __init__(self, datastream, sorted_reqs) -> None:
        super().__init__(datastream.quokka_context, datastream.schema, datastream.source_node_id, sorted_reqs, datastream.materialized)
    
    def __str__(self):
        return "OrderedStream[" + ",".join(self.schema) + "] order by {}".format(self.sorted_reqs.keys())

    def __repr__(self):
        return "OrderedStream[" + ",".join(self.schema) + "] order by {}".format(self.sorted_reqs.keys())

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
    
    def pattern_recognize(self, anchors, duration_limits, partition_by = None):

        """
        experimental API.
        """

        query = "select "
        curr_alias = 0
        for anchor in anchors:
            assert type(anchor) == Expression or type(anchor) == str
            if type(anchor) == Expression:
                anchor = anchor.sql()
            columns = set(i.name for i in anchor.find_all(
            sqlglot.expressions.Column))
            for column in columns:
                assert column in self.schema, "Tried to define an anchor using a column not in the schema {}".format(column)
            query += anchor + " as __anchor_{}, ".format(curr_alias)
            curr_alias += 1
            assert "__anchor_{}".format(curr_alias) not in self.schema, "Column called __anchor_{} already exists in the schema".format(curr_alias)
        query = query[:-2] + " from batch_arrow"

        for duration_limit in duration_limits:
            assert(type(duration_limit) == int or type(duration_limit) == float)

        duration_buffer = sum(duration_limits)
        assert len(duration_limits) == len(anchors) - 1

        class CEPExecutor(Executor):
            def __init__(self) -> None:
                import ldbpy
                self.state = None
                self.cep = ldbpy.CEP(duration_limits)
                self.con = duckdb.connect().execute('PRAGMA threads=%d' % 8)
                self.num_anchors = len(anchors)

            def execute(self,batches,stream_id, executor_id):
                from pyarrow.cffi import ffi
                os.environ["OMP_NUM_THREADS"] = "8"
         
                arrow_batch = pa.concat_tables(batches)
                # you can only process up to the duration_buffer, the rest needs to be cached
                if self.state is not None:
                    arrow_batch = pa.concat_tables([self.state, arrow_batch])
                
                if len(arrow_batch) > duration_buffer:
                    self.state = arrow_batch[-duration_buffer:]
                    arrow_batch = arrow_batch[: -duration_buffer]
                else:
                    self.state = arrow_batch
                    return
            
                result = polars.from_arrow(self.con.execute(query).arrow())
                array_ptrs = []
                schema_ptrs = []
                c_schemas = []
                c_arrays = []
                list_of_arrs = []
                for anchor in range(self.num_anchors):
                    index = result.select(polars.arg_where(polars.col("__anchor_{}".format(anchor))))
                    list_of_arrs.append(index.to_arrow()["__anchor_{}".format(anchor)].combine_chunks())
                    c_schema = ffi.new("struct ArrowSchema*")
                    c_array = ffi.new("struct ArrowArray*")
                    c_schemas.append(c_schema)
                    c_arrays.append(c_array)
                    schema_ptr = int(ffi.cast("uintptr_t", c_schema))
                    array_ptr = int(ffi.cast("uintptr_t", c_array))
                    list_of_arrs[-1]._export_to_c(array_ptr, schema_ptr)
                    array_ptrs.append(array_ptr)
                    schema_ptrs.append(schema_ptr)

                result = self.cep.do_arrow_batch(array_ptrs, schema_ptrs)
                del c_schemas
                del c_arrays
                # print("TIME", time.time() - start)
                
            def done(self,executor_id):
                from pyarrow.cffi import ffi
                if self.state is None:
                    return 

                arrow_batch = self.state
                self.state = None
                result = polars.from_arrow(self.con.execute(query).arrow())
                array_ptrs = []
                schema_ptrs = []
                c_schemas = []
                c_arrays = []
                list_of_arrs = []
                for anchor in range(self.num_anchors):
                    index = result.select(polars.arg_where(polars.col("__anchor_{}".format(anchor))))
                    list_of_arrs.append(index.to_arrow()["__anchor_{}".format(anchor)].combine_chunks())
                    c_schema = ffi.new("struct ArrowSchema*")
                    c_array = ffi.new("struct ArrowArray*")
                    c_schemas.append(c_schema)
                    c_arrays.append(c_array)
                    schema_ptr = int(ffi.cast("uintptr_t", c_schema))
                    array_ptr = int(ffi.cast("uintptr_t", c_array))
                    list_of_arrs[-1]._export_to_c(array_ptr, schema_ptr)
                    array_ptrs.append(array_ptr)
                    schema_ptrs.append(schema_ptr)

                result = self.cep.do_arrow_batch(array_ptrs, schema_ptrs)

        

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
        