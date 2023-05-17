from pyquokka.executors.base_executor import * 

class UDFExecutor:
    def __init__(self, udf) -> None:
        self.udf = udf

    def serialize(self):
        return {}, "all"
    
    def deserialize(self, s):
        pass

    def execute(self,batches,stream_id, executor_id):
        batches = [i for i in batches if i is not None]
        if len(batches) > 0:
            return self.udf(polars.concat(batches, rechunk=False))
        else:
            return None

    def done(self,executor_id):
        return

# this is not fault tolerant. If the storage is lost you just re-read
class StorageExecutor(Executor):
    def __init__(self) -> None:
        pass
    def serialize(self):
        return {}, "all"
    def deserialize(self, s):
        pass
    
    def execute(self,batches,stream_id, executor_id):
        # print("executing storage node")
        batches = [batch for batch in batches if batch is not None and len(batch) > 0]
        #print(batches)
        if len(batches) > 0:
            if type(batches[0]) == polars.DataFrame:
                return polars.concat(batches)
            else:
                return polars.concat([polars.from_arrow(batch) for batch in batches])

    def done(self,executor_id):
        return

class ConcatThenSQLExecutor(Executor):
    def __init__(self, sql_statement) -> None:
        self.statement = sql_statement
        self.state = None

    def checkpoint(self, conn, actor_id, channel_id, seq):
        pass
    
    def restore(self, conn, actor_id, channel_id, seq):
        pass

    def execute(self, batches, stream_id, executor_id):
        batch = pa.concat_tables(batches)
        self.state = batch if self.state is None else pa.concat_tables([self.state, batch])
    
    def done(self, executor_id):
        if self.state is None:
            return None
        con = duckdb.connect().execute('PRAGMA threads=%d' % 8)
        batch_arrow = self.state
        self.state = polars.from_arrow(con.execute(self.statement).arrow())
        del batch_arrow        
        return self.state

class CountExecutor(Executor):
    def __init__(self) -> None:

        self.state = 0

    def checkpoint(self, conn, actor_id, channel_id, seq):
        pass
    
    def restore(self, conn, actor_id, channel_id, seq):
        pass

    def execute(self, batches, stream_id, executor_id):
        
        self.state += sum(len(batch) for batch in batches)
    
    def done(self, executor_id):
        #print("COUNT:", self.state)
        return polars.DataFrame([self.state])

class SuperFastSortExecutor(Executor):
    def __init__(self, key, record_batch_rows = 100000, output_batch_rows = 1000000, file_prefix = "mergesort") -> None:
        self.key = key
        self.record_batch_rows = record_batch_rows
        self.output_batch_rows = output_batch_rows
        self.fileno = 0
        self.prefix = file_prefix # make sure this is different for different executors
        self.data_dir = "/data/"
        self.in_mem_state = None
        self.executor = None

    def write_out_df_to_disk(self, target_filepath, input_mem_table):
        arrow_table = input_mem_table.to_arrow()
        batches = arrow_table.to_batches(1000000)
        writer =  pa.ipc.new_file(pa.OSFile(target_filepath, 'wb'), arrow_table.schema)
        for batch in batches:
            writer.write(batch)
        writer.close()
        # input_mem_table.write_parquet(target_filepath, row_group_size = self.record_batch_rows, use_pyarrow =True)

        return True

    def execute(self, batches, stream_id, executor_id):


        # if self.executor is None:
        #     self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

        # we are going to update the in memory index and flush out the sorted stuff
        
        flush_file_name = self.data_dir + self.prefix + "-" + str(executor_id) + "-" + str(self.fileno) + ".arrow"
        batches = [polars.from_arrow(i) for i in batches if i is not None and len(i) > 0]
        if len(batches) == 0:
            return None
        
        start = time.time()
        batch = polars.concat(batches)
        print("concat execute used", time.time() - start)

        start = time.time()
        sorted_batch = batch.sort(self.key)
        print("sort execute used", time.time() - start)
        
        start = time.time()
        self.write_out_df_to_disk(flush_file_name, sorted_batch)
        # future = self.executor.submit(self.write_out_df_to_disk, flush_file_name, sorted_batch)
        print("flush execute used", time.time() - start)

        start = time.time()
        new_in_mem_state = polars.from_dict({ "values": sorted_batch[self.key], "file_no": np.ones(len(batch), dtype=np.int32) * self.fileno})
        if self.in_mem_state is None:
            self.in_mem_state = new_in_mem_state
        else:
            self.in_mem_state.vstack(new_in_mem_state, in_place=True)
        
        print("update execute state used", time.time() - start)
        
        # assert future.result()
        self.fileno += 1
        
    
    def done(self, executor_id):

        # first sort the in memory state
        print("STARTING DONE", time.time())
        self.in_mem_state = self.in_mem_state.sort("values")
        
        # load the cache
        num_sources = self.fileno 
        sources =  {i : pa.ipc.open_file(pa.memory_map( self.data_dir + self.prefix + "-" + str(executor_id) + "-" + str(i) + ".arrow"  , 'rb')) for i in range(num_sources)}
        number_of_batches_in_source = { source: sources[source].num_record_batches for source in sources}
        cached_batches = {i : polars.from_arrow( pa.Table.from_batches([sources[i].get_batch(0)]) ) for i in sources}
        current_number_for_source = {i: 1 for i in sources}

        print("END DONE SETUP", time.time())

        # now start assembling batches of the output
        for k in range(0, len(self.in_mem_state), self.output_batch_rows):

            start = time.time()

            things_to_get = self.in_mem_state[k : k + self.output_batch_rows]
            file_requirements = things_to_get.groupby("file_no").count()
            desired_batches = []
            for i in range(len(file_requirements)):
                desired_length = file_requirements["count"][i]
                source = file_requirements["file_no"][i]
                while desired_length > len(cached_batches[source]):
                    if current_number_for_source[source] == number_of_batches_in_source[source]:
                        raise Exception
                    else:
                        cached_batches[source].vstack(polars.from_arrow( pa.Table.from_batches( [sources[source].get_batch(current_number_for_source[source])])), in_place=True)
                        current_number_for_source[source] += 1
                else:
                    desired_batches.append(cached_batches[source][:desired_length])
                    cached_batches[source] = cached_batches[source][desired_length:]
            
            result = polars.concat(desired_batches).sort(self.key)
            print("yield one took", time.time() - start)
            yield result

class OutputExecutor(Executor):
    def __init__(self, filepath, format, prefix = "part", region = "local", row_group_size = 5500000) -> None:
        self.num = 0
        assert format == "csv" or format == "parquet"
        self.format = format
        self.filepath = filepath
        self.prefix = prefix
        self.row_group_size = row_group_size
        self.my_batches = []
        self.name = 0
        self.region = region
        self.executor = None

    def upload_write_batch(self, write_batch, executor_id):

        if self.executor is None:
            self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count())
            self.fs = LocalFileSystem() if self.region == "local" else S3FileSystem(region=self.region)

        def upload_parquet(table, where):
            pq.write_table(table, where, filesystem=self.fs)
            return True
        def upload_csv(table, where):
            f = self.fs.open_output_stream(where)
            csv.write_csv(table, f)
            f.close()
            return True
        
        if self.format == "csv":
            for i, (col_name, type_) in enumerate(zip(write_batch.schema.names, write_batch.schema.types)):
                if pa.types.is_decimal(type_):
                    write_batch = write_batch.set_column(i, col_name, compute.cast(write_batch.column(col_name), pa.float64()))

        futures = []

        for i in range(0, len(write_batch), self.row_group_size):
            current_batch = write_batch[i : i + self.row_group_size]
            basename_template = self.filepath + "/" + self.prefix + "-" + str(executor_id) + "-" + str(self.name)  + "." + self.format
            self.name += 1
            if self.format == "parquet":
                futures.append(self.executor.submit(upload_parquet, current_batch, basename_template))
            else:
                futures.append(self.executor.submit(upload_csv, current_batch, basename_template))
        
        assert all([fut.result() for fut in futures])

    def execute(self,batches,stream_id, executor_id):

        import numpy as np

        self.my_batches.extend([i for i in batches if i is not None and len(i) > 0])

        lengths = [len(batch) for batch in self.my_batches]
        total_len = np.sum(lengths)

        if total_len <= self.row_group_size:
            return

        write_len = total_len // self.row_group_size * self.row_group_size
        full_batches_to_take = np.where(np.cumsum(lengths) >= write_len)[0][0]        

        write_batch = pa.concat_tables(self.my_batches[:full_batches_to_take]) if full_batches_to_take > 0 else None
        rows_to_take = int(write_len - np.sum(lengths[:full_batches_to_take]))
        self.my_batches = self.my_batches[full_batches_to_take:]
        if rows_to_take > 0:
            if write_batch is not None:
                write_batch = pa.concat_tables([write_batch, self.my_batches[0][:rows_to_take]])
            else:
                write_batch = self.my_batches[0][:rows_to_take]
            self.my_batches[0] = self.my_batches[0][rows_to_take:]

        assert len(write_batch) % self.row_group_size == 0
        # print("WRITING", self.filepath,self.mode )

        self.upload_write_batch(write_batch, executor_id)

        return_df = polars.from_dict({"filename":[(self.prefix + "-" + str(executor_id) + "-" + str(self.name) + "-" + str(i) + "." + self.format) for i in range(len(write_batch) // self.row_group_size) ]})
        return return_df

    def done(self,executor_id):
        df = pa.concat_tables(self.my_batches)
        self.upload_write_batch(df, executor_id)
        
        return_df = polars.from_dict({"filename":[(self.prefix + "-" + str(executor_id) + "-" + str(self.name) + "-" + str(i) + "." + self.format) for i in range((len(df) -1) // self.row_group_size + 1) ]})
        return return_df

class BroadcastJoinExecutor(Executor):
    # batch func here expects a list of dfs. This is a quark of the fact that join results could be a list of dfs.
    # batch func must return a list of dfs too
    def __init__(self, small_table, on = None, small_on = None, big_on = None, suffix = "_small", how = "inner"):

        self.suffix = suffix

        assert how in {"inner", "left", "semi", "anti"}
        self.how = how

        if type(small_table) == pd.core.frame.DataFrame:
            self.state = polars.from_pandas(small_table)
        elif type(small_table) == polars.DataFrame:
            self.state = small_table
        else:
            raise Exception("small table data type not accepted")

        if on is not None:
            assert small_on is None and big_on is None
            self.small_on = on
            self.big_on = on
        else:
            assert small_on is not None and big_on is not None
            self.small_on = small_on
            self.big_on = big_on
        
        assert self.small_on in self.state.columns
    
    def checkpoint(self, conn, actor_id, channel_id, seq):
        pass
    
    def restore(self, conn, actor_id, channel_id, seq):
        pass

    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batches, stream_id, executor_id):
        # state compaction
        batches = [polars.from_arrow(i) for i in batches if i is not None and len(i) > 0]
        if len(batches) == 0:
            return
        batch = polars.concat(batches)
        return batch.join(self.state, left_on = self.big_on, right_on = self.small_on, how = self.how, suffix = self.suffix)
        
    def done(self,executor_id):
        return

# this is an inner join executor that must return outputs in a sorted order based on sorted_col
# the operator will maintain the sortedness of the probe side
# 0/left is probe, 1/right is build.
class BuildProbeJoinExecutor(Executor):

    def __init__(self, on = None, left_on = None, right_on = None, how = "inner", key_to_keep = "left"):

        self.state = None

        if on is not None:
            assert left_on is None and right_on is None
            self.left_on = on
            self.right_on = on
        else:
            assert left_on is not None and right_on is not None
            self.left_on = left_on
            self.right_on = right_on
        
        self.phase = "build"
        assert how in {"inner", "left", "semi", "anti"}
        self.how = how
        self.key_to_keep = key_to_keep
        self.things_seen = []

    def execute(self,batches, stream_id, executor_id):
        # state compaction
        batches = [polars.from_arrow(i) for i in batches if i is not None and len(i) > 0]
        if len(batches) == 0:
            return
        batch = polars.concat(batches)
        self.things_seen.append((stream_id, len(batches)))

        # build
        if stream_id == 1:
            assert self.phase == "build", (self.left_on, self.right_on, self.things_seen)
            self.state = batch if self.state is None else self.state.vstack(batch, in_place = True)
               
        # probe
        elif stream_id == 0:
            if self.state is None:
                if self.how == "anti":
                    return batch
                else:
                    return
            # print("STATE LEN", len(self.state))
            if self.phase == "build":
                self.state = self.state.sort(self.right_on)
            self.phase = "probe"
            result = batch.join(self.state,left_on = self.left_on, right_on = self.right_on ,how= self.how)
            if self.key_to_keep == "right":
                result = result.rename({self.left_on: self.right_on})
            return result
    
    def done(self,executor_id):
        pass

class DistinctExecutor(Executor):
    def __init__(self, keys) -> None:

        self.keys = keys
        self.state = None
    
    def checkpoint(self, conn, actor_id, channel_id, seq):
        pass
    
    def restore(self, conn, actor_id, channel_id, seq):
        pass

    def execute(self, batches, stream_id, executor_id):
        
        batches = [polars.from_arrow(i) for i in batches if i is not None and len(i) > 0]
        if len(batches) == 0:
            return
        batch = polars.concat(batches)
        batch = batch.unique()

        if self.state is None:
            self.state = batch
            return batch
        else:
            contribution = batch.join(self.state, on = self.keys, how="anti")
            self.state.vstack(contribution, in_place = True)
            return contribution
    
    def serialize(self):
        return {0:self.seen}, "all"
    
    def deserialize(self, s):
        # the default is to get a list of things 
        assert type(s) == list and len(s) == 1
        self.seen = s[0][0]
    
    def done(self, executor_id):
        return

class SQLAggExecutor(Executor):
    def __init__(self, groupby_keys, orderby_keys, sql_statement) -> None:
        """
        groupby_keys (list): Keys to perform the group by operation on.
        orderby_keys (list of tuples): Keys to order by. Add "desc" as second element of tuple to order in descending order.
        sql_statement (string): aggregation statement in SQL
        """
        assert type(groupby_keys) == list
        if orderby_keys is not None:
            assert type(orderby_keys) == list
        if len(groupby_keys) > 0:
            self.agg_clause = "select " + ",".join(groupby_keys) + ", " + sql_statement + " from batch_arrow"
        else:
            self.agg_clause = "select " + sql_statement + " from batch_arrow"
        if len(groupby_keys) > 0:
            self.agg_clause += " group by "
            for key in groupby_keys:
                self.agg_clause += key + ","
            self.agg_clause = self.agg_clause[:-1]

        if orderby_keys is not None and len(orderby_keys) > 0:
            self.agg_clause += " order by "
            for key, dir in orderby_keys:
                if dir == "desc":
                    self.agg_clause += key + " desc,"
                else:
                    self.agg_clause += key + ","
            self.agg_clause = self.agg_clause[:-1]
        
        self.state = None
    
    def execute(self, batches, stream_id, executor_id):
        batch = pa.concat_tables(batches)
        self.state = batch if self.state is None else pa.concat_tables([self.state, batch])

    def done(self, executor_id):
        if self.state is None:
            return None
        con = duckdb.connect().execute('PRAGMA threads=%d' % 8)
        batch_arrow = self.state
        self.state = polars.from_arrow(con.execute(self.agg_clause).arrow())
        del batch_arrow        
        return self.state
