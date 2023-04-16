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
        