from pyquokka.executors.base_executor import * 

"""
We are going to support four kinds of windows:
- hopping (defined by window length, hop size)
- sliding (defined by window length)
- session (defined by gap length)

We will expect the batches to come in sorted order.
"""

class HoppingWindowExecutor(Executor):
    def __init__(self, time_col, by_col, window,  trigger) -> None:
        self.time_col = time_col
        self.by_col = by_col
        self.state = None
        assert issubclass(type(window), HoppingWindow)
        assert issubclass(type(trigger), Trigger)
        self.window = window
        self.trigger = trigger

        # hopping window - event trigger is not supported. It is very complicated and probably not worth it.
        if type(trigger) == OnEventTrigger and type(window) == HoppingWindow:
            raise Exception("OnEventTrigger is not supported for hopping windows")

    def execute(self, batches, stream_id, executor_id):
        
        batches = [polars.from_arrow(i) for i in batches if i is not None and len(i) > 0]
        batch = polars.concat(batches)

        # current polars implementation cannot support floating point groupby dynamic and rolling operations.
        assert (batch[self.time_col].dtype == polars.Int32 or batch[self.time_col].dtype == polars.Int64 or 
        batch[self.time_col].dtype == polars.Datetime or batch[self.time_col].dtype == polars.Date), batch[self.time_col].dtype

        size = self.window.size_polars
        hop = self.window.hop_polars            
        result = None

        # for a hopping window, we want to make sure that we delegate all the rows in uncompleted windows to the next execute call.
        # therefore we need to compute the end time of the last completed window. 
        timestamp_of_last_row = batch[self.time_col][-1]
        if type(timestamp_of_last_row) == datetime.datetime:
            last_start = (timestamp_of_last_row - self.window.size).timestamp() // self.window.hop.total_seconds() * self.window.hop.total_seconds()
            last_end = last_start + self.window.size.total_seconds()
            new_state = batch.filter(polars.col(self.time_col) > datetime.datetime.fromtimestamp(last_end))
            batch = batch.filter(polars.col(self.time_col) <= datetime.datetime.fromtimestamp(last_end))
            
        elif type(timestamp_of_last_row) == int:
            last_start = (timestamp_of_last_row - self.window.size) // self.window.hop * self.window.hop
            last_end = last_start + self.window.size
            new_state = batch.filter(polars.col(self.time_col) > last_end)
            batch = batch.filter(polars.col(self.time_col) <= last_end)
        else:
            raise NotImplementedError
        
        if self.state is not None:
            batch = polars.concat([self.state, batch])
        self.state = new_state

        if type(self.trigger) == OnCompletionTrigger:
            # we are going to use polars groupby dynamic
            result = batch.groupby_dynamic(self.time_col, every = hop, period= size, by = self.by_col).agg(self.window.polars_aggregations()).sort(self.time_col)

        elif type(self.trigger) == OnEventTrigger:
        
            # we will assign a window id to each row, then use DuckDB's SQL window functions.
            # this is not the most efficient way to do this, but it is the easiest.

            assert type(self.window) == TumblingWindow
            if timestamp_of_last_row == datetime.datetime:
                batch = batch.with_column((polars.col(self.time_col).cast(polars.Int64) // self.window.size.total_seconds()).alias("__window_id"))
            else:
                batch = batch.with_column((polars.col(self.time_col) // self.window.size).alias("__window_id"))

            batch_arrow = batch.to_arrow()

            aggregations = self.window.sql_aggregations()
            con = duckdb.connect().execute('PRAGMA threads=%d' % 8)

            result = con.execute("""
                SELECT 
                    BY_COL,
                    TIME_COL,
                    AGG_FUNCS
                FROM batch_arrow
                WINDOW win AS (
                    PARTITION BY BY_COL, __window_id
                    ORDER BY TIME_COL
                    RANGE unbounded preceding
                )
            """.replace("TIME_COL", self.time_col).replace("BY_COL", self.by_col).replace("AGG_FUNCS", aggregations)).arrow()

            result = polars.from_arrow(result)
    
        else:
            raise NotImplementedError("unrecognized trigger type")
        
        return result

    def done(self, executor_id):

        if type(self.trigger) == OnCompletionTrigger:
            size = self.window.size_polars
            hop = self.window.hop_polars
            if self.state is not None and len(self.state) > 0:
                result = self.state.groupby_dynamic(self.time_col, every = hop, period= size, by = self.by_col).agg(self.aggregations).sort(self.time_col)
            else:
                result = None
        elif type(self.trigger) == OnEventTrigger:
            assert type(self.window) == TumblingWindow
            if self.state is not None and len(self.state) > 0:
                batch = self.state
                timestamp_of_last_row = batch[self.time_col][-1]
                if timestamp_of_last_row == datetime.datetime:
                    batch = batch.with_column((polars.col(self.time_col).cast(polars.Int64) // self.window.size.total_seconds()).alias("__window_id"))
                else:
                    batch = batch.with_column((polars.col(self.time_col) // self.window.size).alias("__window_id"))

                batch_arrow = batch.to_arrow()

                aggregations = self.window.sql_aggregations()
                con = duckdb.connect().execute('PRAGMA threads=%d' % 8)

                result = con.execute("""
                    SELECT 
                        BY_COL,
                        TIME_COL,
                        AGG_FUNCS
                    FROM batch_arrow
                    WINDOW win AS (
                        PARTITION BY BY_COL, __window_id
                        ORDER BY TIME_COL
                        RANGE unbounded preceding
                    )
                """.replace("TIME_COL", self.time_col).replace("BY_COL", self.by_col).replace("AGG_FUNCS", aggregations)).arrow()

                result = polars.from_arrow(result)
            else:
                result = None

        else:
            raise NotImplementedError("unrecognized trigger type")
        
        self.state = None
        return result

class SlidingWindowExecutor(Executor):
    def __init__(self, time_col, by_col, window,  trigger) -> None:
        self.time_col = time_col
        self.by_col = by_col
        self.state = None
        assert issubclass(type(window), SlidingWindow)
        assert issubclass(type(trigger), Trigger)
        self.window = window
        self.trigger = trigger

        # hopping window - event trigger is not supported. It is very complicated and probably not worth it.
        if type(trigger) == OnCompletionTrigger:
            print("Trying to use completion trigger with sliding window. This will result in the same behavior as an OnEventTrigger.")
            print("The completion time of a sliding window is when the last event comes, so they are the same. Timeout for completion trigger is ignored currently.")
        

    def execute(self, batches, stream_id, executor_id):

        batches = [polars.from_arrow(i) for i in batches if i is not None and len(i) > 0]
        batch = polars.concat(batches)

        # current polars implementation cannot support floating point groupby dynamic and rolling operations.
        assert (batch[self.time_col].dtype == polars.Int32 or batch[self.time_col].dtype == polars.Int64 or 
        batch[self.time_col].dtype == polars.Datetime or batch[self.time_col].dtype == polars.Date), batch[self.time_col].dtype

        size = self.window.size_before_polars
        to_discard = None
        if self.state is not None:
            batch = polars.concat([self.state, batch], rechunk=True)
            to_discard = len(self.state)

        timestamp_of_last_row = batch[self.time_col][-1]
        # python dynamic typing -- this will work for both timedelta window size and int window size
        self.state = batch.filter(polars.col(self.time_col) > timestamp_of_last_row - self.window.size_before)
        # print(len(self.state))
        # partitions = batch.partition_by(self.by_col)
        # results = []
        # for partition in partitions:
        #     results.append(partition.groupby_rolling(self.time_col, period = size).agg(self.window.polars_aggregations()))
        # result = polars.concat(results)
        result = batch.groupby_rolling(self.time_col, period= size, by = self.by_col).agg(self.window.polars_aggregations())#.sort(self.time_col)
        if to_discard is not None:
            result = result[to_discard:]

        return result
    
    def done(self, executor_id):
        return None


class SessionWindowExecutor(Executor):
    def __init__(self, time_col, by_col, window,  trigger) -> None:
        self.time_col = time_col
        self.by_col = by_col
        self.state = None
        assert issubclass(type(window), SessionWindow)
        assert issubclass(type(trigger), Trigger)
        self.window = window
        self.trigger = trigger

        # hopping window - event trigger is not supported. It is very complicated and probably not worth it.
        if type(trigger) == OnCompletionTrigger:
            print("Trying to use completion trigger with sliding window. This will result in the same behavior as an OnEventTrigger.")
            print("The completion time of a sliding window is when the last event comes, so they are the same. Timeout for completion trigger is ignored currently.")
    
    def execute(self, batches, stream_id, executor_id):
        batches = [polars.from_arrow(i) for i in batches if i is not None and len(i) > 0]
        batch = polars.concat(batches)

        # current polars implementation cannot support floating point groupby dynamic and rolling operations.
        assert (batch[self.time_col].dtype == polars.Int32 or batch[self.time_col].dtype == polars.Int64 or 
        batch[self.time_col].dtype == polars.Datetime or batch[self.time_col].dtype == polars.Date), batch[self.time_col].dtype
        timeout = self.window.timeout

        if self.state is not None:
            batch = polars.concat([self.state, batch])

        lazy_batch = batch.lazy()
        windowed_batch = lazy_batch.select([self.time_col, self.by_col]).groupby(self.by_col).agg(
            [
                polars.col(self.time_col),
                (polars.col("ts") - polars.col("ts").shift(1) > timeout).cumsum().alias("__window_id"),
            ]
        ).explode([self.time_col, "__window_id"]).fill_null(0).join(lazy_batch, on = [self.by_col, self.time_col]).collect()

        # you will need to collect rows corresponding to the last window id for each of the elements in by_col

        last_window_id = windowed_batch.groupby(self.by_col).agg(polars.max("__window_id"))
        # now collect the rows in windowed batch with last_window_id
        self.state = windowed_batch.join(last_window_id, on = [self.by_col, "__window_id"]).drop("__window_id")
        windowed_batch = windowed_batch.join(last_window_id, on = [self.by_col, "__window_id"], how = "anti")        

        if type(self.trigger) == OnCompletionTrigger:
            result = windowed_batch.groupby([self.by_col, "__window_id"]).agg(self.window.polars_aggregations())
        elif type(self.trigger) == OnEventTrigger:
            batch_arrow = windowed_batch.to_arrow()

            aggregations = self.window.sql_aggregations()
            con = duckdb.connect().execute('PRAGMA threads=%d' % 8)

            result = con.execute("""
                SELECT 
                    BY_COL,
                    TIME_COL,
                    AGG_FUNCS
                FROM batch_arrow
                WINDOW win AS (
                    PARTITION BY BY_COL, __window_id
                    ORDER BY TIME_COL
                    RANGE unbounded preceding
                )
            """.replace("TIME_COL", self.time_col).replace("BY_COL", self.by_col).replace("AGG_FUNCS", aggregations)).arrow()

        return result

    def done(self, executor_id):
        
        if self.state is None or len(self.state) == 0:
            return 
        else:
            if type(self.trigger) == OnCompletionTrigger:
                result = self.state.with_column(polars.lit(1).alias("__window_id")).groupby("__window_id").agg(self.window.polars_aggregations())
            elif type(self.trigger) == OnEventTrigger:
                batch_arrow = self.state.to_arrow()

                aggregations = self.window.sql_aggregations()
                con = duckdb.connect().execute('PRAGMA threads=%d' % 8)

                result = con.execute("""
                    SELECT 
                        BY_COL,
                        TIME_COL,
                        AGG_FUNCS
                    FROM batch_arrow
                    WINDOW win AS (
                        PARTITION BY BY_COL, __window_id
                        ORDER BY TIME_COL
                        RANGE unbounded preceding
                    )
                """.replace("TIME_COL", self.time_col).replace("BY_COL", self.by_col).replace("AGG_FUNCS", aggregations)).arrow()
        
            return result

class BroadcastAsofJoinExecutor(Executor):
    # the broadcast asof join only supports if you are proving the quotes 
    def __init__(self, small_quotes, time_col_trades = 'time', time_col_quotes = 'time', symbol_col_trades = 'symbol', symbol_col_quotes = 'symbol', suffix = "_right"):

        self.suffix = suffix

        assert type(small_table) == polars.DataFrame
        self.state = small_table

        self.time_col_trades = time_col_trades
        self.time_col_quotes = time_col_quotes
        self.symbol_col_trades = symbol_col_trades
        self.symbol_col_quotes = symbol_col_quotes
        
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

class SortedAsofExecutor(Executor):
    def __init__(self, time_col_trades = 'time', time_col_quotes = 'time', symbol_col_trades = 'symbol', symbol_col_quotes = 'symbol', suffix = "_right") -> None:
        self.trade_state = None
        self.quote_state = None
        self.join_state = None
        self.time_col_trades = time_col_trades
        self.time_col_quotes = time_col_quotes
        self.symbol_col_trades = symbol_col_trades
        self.symbol_col_quotes = symbol_col_quotes
        self.suffix = suffix

    def execute(self,batches,stream_id, executor_id):    
        # sort_col = self.time_col_trades if stream_id == 0 else self.time_col_quotes
        # batch = polars.from_arrow(pa.concat_tables([batch.sort_by(sort_col) for batch in batches]))
        batch = polars.from_arrow(pa.concat_tables(batches))
        if stream_id == 0:
            # assert batch[self.time_col_trades].is_sorted()
            if self.trade_state is None:
                self.trade_state = batch
            else:
                if len(self.trade_state) > 0:
                    assert self.trade_state[self.time_col_trades][-1] <= batch[self.time_col_trades][0]
                self.trade_state.vstack(batch, in_place = True)
        else:
            # assert batch[self.time_col_quotes].is_sorted()
            if self.quote_state is None:
                self.quote_state = batch
            else:
                if len(self.quote_state) > 0:
                    assert self.quote_state[self.time_col_quotes][-1] <= batch[self.time_col_quotes][0]
                self.quote_state.vstack(batch, in_place = True)

        if self.trade_state is None or self.quote_state is None or len(self.trade_state) == 0 or len(self.quote_state) == 0:
            return

        joinable_trades = self.trade_state.filter(polars.col(self.time_col_trades) < self.quote_state[self.time_col_quotes][-1])
        if len(joinable_trades) == 0:
            return
        
        joinable_quotes = self.quote_state.filter(polars.col(self.time_col_quotes) <= joinable_trades[self.time_col_trades][-1])
        if len(joinable_quotes) == 0:
            return

        self.trade_state =  self.trade_state.filter(polars.col(self.time_col_trades) >= self.quote_state[self.time_col_quotes][-1])

        result = joinable_trades.join_asof(joinable_quotes, left_on = self.time_col_trades, right_on = self.time_col_quotes, by_left = self.symbol_col_trades, by_right = self.symbol_col_quotes, suffix = self.suffix)

        mock_result = joinable_quotes.join_asof(joinable_trades, left_on = self.time_col_quotes, right_on = self.time_col_trades, by_left = self.symbol_col_quotes, by_right = self.symbol_col_trades, suffix = self.suffix, strategy = "forward").drop_nulls()
        latest_joined_quotes = mock_result.groupby(self.symbol_col_quotes).agg([polars.max(self.time_col_quotes)])
        start = time.time()
        new_quote_state = self.quote_state.join(latest_joined_quotes, on = self.symbol_col_quotes, how = "left", suffix = "_latest").fill_null(-1)
        print("join time: ", time.time() - start)
        self.quote_state = new_quote_state.filter(polars.col(self.time_col_quotes) >= polars.col(self.time_col_quotes + "_latest")).drop([self.time_col_quotes + "_latest"])

        # print(len(result))

        return result
    
    def done(self, executor_id):
        return self.trade_state.join_asof(self.quote_state, left_on = self.time_col_trades, right_on = self.time_col_quotes, by_left = self.symbol_col_trades, by_right = self.symbol_col_quotes, suffix = self.suffix)
