import os
import polars 
class GroupAsOfJoinExecutor():
    # batch func here expects a list of dfs. This is a quark of the fact that join results could be a list of dfs.
    # batch func must return a list of dfs too
    def __init__(self, group_on= None, group_left_on = None, group_right_on = None, on = None, left_on = None, right_on = None, batch_func = None, columns = None, suffix="_right"):

        # how many things you might checkpoint, the number of keys in the dict
        self.num_states = 2

        self.trade = {}
        self.quote = {}
        self.ckpt_start0 = 0
        self.ckpt_start1 = 0
        self.columns = columns
        self.suffix = suffix

        if on is not None:
            assert left_on is None and right_on is None
            self.left_on = on
            self.right_on = on
        else:
            assert left_on is not None and right_on is not None
            self.left_on = left_on
            self.right_on = right_on
        
        if group_on is not None:
            assert group_left_on is None and group_right_on is None
            self.group_left_on = group_on
            self.group_right_on = group_on
        else:
            assert group_left_on is not None and group_right_on is not None
            self.group_left_on = group_left_on
            self.group_right_on = group_right_on

        self.batch_func = batch_func
        # keys that will never be seen again, safe to delete from the state on the other side

    def serialize(self):
        result = {0:self.trade, 1:self.quote}        
        return result, "all"
    
    def deserialize(self, s):
        assert type(s) == list
        self.trade = s[0][0]
        self.quote = s[0][1]
    
    def find_second_smallest(self, batch, key):
        smallest = batch[0][key]
        for i in range(len(batch)):
            if batch[i][key] > smallest:
                return batch[i][key]
    
    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batches, stream_id, executor_id):
        # state compaction
        batches = [i for i in batches if len(i) > 0]
        if len(batches) == 0:
            return
        
        # self.trade will be a dictionary of lists. 
        # self.quote will be a dictionary of lists.

        # trade
        ret_vals = []
        if stream_id == 0:
            for batch in batches:
                frames = batch.partition_by(self.group_left_on)
                for trade_chunk in frames:
                    symbol = trade_chunk["symbol"][0]
                    min_trade_ts = trade_chunk[self.left_on][0]
                    max_trade_ts = trade_chunk[self.left_on][-1]
                    if symbol not in self.quote:
                        if symbol in self.trade:
                            self.trade[symbol].append(trade_chunk)
                        else:
                            self.trade[symbol] = [trade_chunk]
                        continue
                    current_quotes_for_symbol = self.quote[symbol]
                    for i in range(len(current_quotes_for_symbol)):
                        quote_chunk = current_quotes_for_symbol[i]
                        min_quote_ts = quote_chunk[self.right_on][0]
                        max_quote_ts = quote_chunk[self.right_on][-1]
                        #print(max_trade_ts, min_quote_ts, min_trade_ts, max_quote_ts)
                        if max_trade_ts < min_quote_ts or min_trade_ts > max_quote_ts:
                            # no overlap.
                            continue
                        else:
                            second_smallest_quote_ts = self.find_second_smallest(quote_chunk, self.right_on)
                            joinable_trades = trade_chunk[(trade_chunk[self.left_on] >= second_smallest_quote_ts) & (trade_chunk[self.left_on] < max_quote_ts)]
                            if len(joinable_trades) == 0:
                                continue
                            trade_start_ts = joinable_trades[self.left_on][0]
                            trade_end_ts = joinable_trades[self.left_on][-1]
                            if len(joinable_trades) == 0:
                                continue
                            quote_start_ts = quote_chunk[self.right_on][quote_chunk[self.right_on] <= trade_start_ts][-1]
                            quote_end_ts = quote_chunk[self.right_on][quote_chunk[self.right_on] <= trade_end_ts][-1]
                            joinable_quotes = quote_chunk[(quote_chunk[self.right_on] >= quote_start_ts) & (quote_chunk[self.right_on] <= quote_end_ts)]
                            if len(joinable_quotes) == 0:
                                continue
                            trade_chunk = trade_chunk[(trade_chunk[self.left_on] < trade_start_ts) | (trade_chunk[self.left_on] > trade_end_ts)]
                            new_chunk = quote_chunk[(quote_chunk[self.right_on] < quote_start_ts) | (quote_chunk[self.left_on] > quote_end_ts)]
                            if len(new_chunk) > 0:
                                self.quote[symbol][i] = new_chunk
                            else:
                                del self.quote[symbol][i]
                            ret_vals.append(joinable_trades.join_asof(joinable_quotes.drop(self.group_right_on), left_on = self.left_on, right_on = self.right_on))
                            if len(trade_chunk) == 0:
                                break
                    if len(trade_chunk) == 0:
                        continue
                    if symbol in self.trade:
                        self.trade[symbol].append(trade_chunk)
                    else:
                        self.trade[symbol] = [trade_chunk]
        #quote
        elif stream_id == 1:
            for batch in batches:
                frames = batch.partition_by(self.group_right_on)
                for quote_chunk in frames:
                    symbol = quote_chunk["symbol"][0]
                    min_quote_ts = quote_chunk[self.right_on][0]
                    max_quote_ts = quote_chunk[self.right_on][-1]
                    if symbol not in self.trade:
                        if symbol in self.quote:
                            self.quote[symbol].append(quote_chunk)
                        else:
                            self.quote[symbol] = [quote_chunk]
                        continue
                        
                    current_trades_for_symbol = self.trade[symbol]
                    for i in range(len(current_trades_for_symbol)):
                        trade_chunk = current_trades_for_symbol[i]
                        #print(current_trades_for_symbol)
                        min_trade_ts = trade_chunk[self.left_on][0]
                        max_trade_ts = trade_chunk[self.left_on][-1]
                        if max_trade_ts < min_quote_ts or min_trade_ts > max_quote_ts:
                            # no overlap.
                            continue
                        else:
                            second_smallest_quote_ts = self.find_second_smallest(quote_chunk, self.right_on)
                            joinable_trades = trade_chunk[(trade_chunk[self.left_on] >= second_smallest_quote_ts) &( trade_chunk[self.left_on] < max_quote_ts)]
                            if len(joinable_trades) == 0:
                                continue
                            trade_start_ts = joinable_trades[self.left_on][0]
                            trade_end_ts = joinable_trades[self.left_on][-1]
                            if len(joinable_trades) == 0:
                                continue
                            quote_start_ts = quote_chunk[self.right_on][quote_chunk[self.right_on] <= trade_start_ts][-1]
                            quote_end_ts = quote_chunk[self.right_on][quote_chunk[self.right_on] <= trade_end_ts][-1]
                            joinable_quotes = quote_chunk[(quote_chunk[self.right_on] >= quote_start_ts) & (quote_chunk[self.right_on] <= quote_end_ts)]
                            if len(joinable_quotes) == 0:
                                continue
                            quote_chunk = quote_chunk[(quote_chunk[self.right_on] < quote_start_ts ) | (quote_chunk[self.left_on] > quote_end_ts)]
                            new_chunk = trade_chunk[(trade_chunk[self.left_on] < trade_start_ts) | (trade_chunk[self.left_on] > trade_end_ts)]
                            if len(new_chunk) > 0:
                                self.trade[symbol][i] = new_chunk
                            else:
                                del self.trade[symbol][i]
                            ret_vals.append(joinable_trades.join_asof(joinable_quotes.drop(self.group_right_on), left_on = self.left_on, right_on = self.right_on))
                            if len(quote_chunk) == 0:
                                break
                    if len(quote_chunk) == 0:
                        continue
                    if symbol in self.quote:
                        self.quote[symbol].append(quote_chunk)
                    else:
                        self.quote[symbol] = [quote_chunk]
        #print(ret_vals)

        if len(ret_vals) == 0:
            return
        result = polars.concat(ret_vals).drop_nulls()
        
        if self.columns is not None and result is not None and len(result) > 0:
            result = result[self.columns]

        if result is not None and len(result) > 0:
            if self.batch_func is not None:
                da =  self.batch_func(result.to_pandas())
                return da
            else:
                print("RESULT LENGTH",len(result))
                return result
    
    def done(self,executor_id):
        #print(len(self.state0),len(self.state1))
        ret_vals = []
        for symbol in self.trade:
            if symbol not in self.quote:
                continue
            else:
                trades = polars.concat(self.trade[symbol]).sort(self.left_on)
                quotes = polars.concat(self.quote[symbol]).sort(self.right_on)
                ret_vals.append(trades.join_asof(quotes.drop(self.group_right_on), left_on = self.left_on, right_on = self.right_on, suffix=self.suffix))
        
        print("done asof join ", executor_id)
        return polars.concat(ret_vals).drop_nulls()

executor = GroupAsOfJoinExecutor(group_on = "symbol", on="time")
quotes = polars.read_csv("test_quote2.csv")
trades = polars.read_csv("test_trade2.csv")
#quotes = quotes[quotes.symbol == "AAL"]
#trades = trades[trades.symbol == "AAL"]
executor.execute([quotes[:1000]], 1, 0)
results = []
results.append(executor.execute([trades[:1000], trades[2000:3000]], 0, 0))
#print(executor.trade, executor.quote)

results.append(executor.execute([quotes[1000:3000]], 1, 0))
results.append(executor.execute([trades[1000:2000]], 0, 0))

results.append(executor.done(0))
result = polars.concat([i for i in results if i is not None]).drop_nulls()
ref = trades[:3000].join_asof(quotes[:3000],by="symbol",on="time").sort("symbol").drop_nulls()
result.to_csv("result.csv")
print(result.sort("symbol"))
print(ref)
print((result["size"] - ref["size"]).sum())
