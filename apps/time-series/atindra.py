import pandas as pd
import polars as pl
import math
import numpy as np
import random
from datetime import datetime

# datetime object containing current date and time
now = datetime.now()
dt_string = now.strftime("%d-%m-%Y_%H:%M")
global trade_dict, quote_dict, result
trade_dict = None
quote_dict = None
result = None
# trade_holder = pl.DataFrame()
# quote_holder = pl.DataFrame()
check_asof = []

class SortedAsofExecutor():
    def __init__(self, time_col_trades = 'time', time_col_quotes = 'time', symbol_col_trades = 'symbol', symbol_col_quotes = 'symbol', suffix = "_right") -> None:
        self.trade_state = None
        self.quote_state = None
        self.join_state = None
        self.time_col_trades = time_col_trades
        self.time_col_quotes = time_col_quotes
        self.symbol_col_trades = symbol_col_trades
        self.symbol_col_quotes = symbol_col_quotes
        self.suffix = suffix
        self.quote_watermark = None
        self.latest_quote_ts_per_symbol = None

    def execute(self,batch,stream_id, executor_id):    
        # sort_col = self.time_col_trades if stream_id == 0 else self.time_col_quotes
        # batch = polars.from_arrow(pa.concat_tables([batch.sort_by(sort_col) for batch in batches]))
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

        joinable_trades = self.trade_state.filter(self.trade_state[self.time_col_trades] < self.quote_state[self.time_col_quotes][-1])
        if len(joinable_trades) == 0:
            return
        
        joinable_quotes = self.quote_state.filter(self.quote_state[self.time_col_quotes] <= joinable_trades[self.time_col_trades][-1])
        if len(joinable_quotes) == 0:
            return

        self.trade_state =  self.trade_state.filter(self.trade_state[self.time_col_trades] >= self.quote_state[self.time_col_quotes][-1])
        # self.quote_state = self.quote_state.filter(self.quote_state[self.time_col_quotes] >= joinable_quotes[self.time_col_quotes][-1])

        result = joinable_trades.join_asof(joinable_quotes, left_on = self.time_col_trades, right_on = self.time_col_quotes, by_left = self.symbol_col_trades, by_right = self.symbol_col_quotes, suffix = self.suffix)

        mock_result = joinable_quotes.join_asof(joinable_trades, left_on = self.time_col_quotes, right_on = self.time_col_trades, by_left = self.symbol_col_quotes, by_right = self.symbol_col_trades, suffix = self.suffix, strategy = "forward").drop_nulls()
        latest_joined_quotes = mock_result.groupby(self.symbol_col_quotes).agg([pl.max(self.time_col_quotes)])
        new_quote_state = self.quote_state.join(latest_joined_quotes, on = self.symbol_col_quotes, how = "left", suffix = "_latest").fill_null(-1)
        self.quote_state = new_quote_state.filter(pl.col(self.time_col_quotes) >= pl.col(self.time_col_quotes + "_latest")).drop([self.time_col_quotes + "_latest"])
        # print(mock_result)


        # print(len(result))
        return result
    
    def done(self, executor_id):
        return self.trade_state.join_asof(self.quote_state, left_on = self.time_col_trades, right_on = self.time_col_quotes, by_left = self.symbol_col_trades, by_right = self.symbol_col_quotes, suffix = self.suffix)

def getBatch(data, total_rows, batch_head, batch_tail):
    if batch_tail < total_rows:
        batch = data[batch_head:batch_tail]
    else:
        batch = data[batch_head:total_rows]
    return batch

    
def GroupAsofMain(tradeFile, quoteFile):
    executor = SortedAsofExecutor()
    trades = pl.read_csv(tradeFile, has_header=True, separator = ",")
    quotes = pl.read_csv(quoteFile, has_header=True, separator = ",")
    trade_rows = len(trades) 
    quote_rows = len(quotes)
    batch_size = 500
    trade_batches = math.ceil(trade_rows/batch_size)
    quote_batches = math.ceil(quote_rows/batch_size)
    counter0, counter1 = 0, 0
    trade_head, trade_tail, quote_head, quote_tail = 0,0,0,0
    result = None
    while counter0 < trade_batches or counter1 < quote_batches:
        if counter0 < trade_batches and counter1 < quote_batches:
            indicator = random.choice([0,1])
        elif counter0 < trade_batches:
            indicator = 0
        elif counter1 < quote_batches:
            indicator = 1
        if indicator == 0:
            trade_head = counter0 * batch_size
            trade_tail = trade_head + batch_size
            batch = getBatch(trades, trade_rows, trade_head, trade_tail)
            counter0 += 1
        else:
            quote_head = counter1 * batch_size
            quote_tail = quote_head + batch_size
            batch = getBatch(quotes, quote_rows, quote_head, quote_tail)
            counter1 += 1
        execute_result = executor.execute(batch, indicator, 0)
        if execute_result is not None:
            if result is None:
                result = execute_result
            else:
                result.vstack(execute_result, in_place = True)


    result.vstack(executor.done(0), in_place = True)


    # print("UNMATCHED TRADES", len(unmatched_trades))
    return result


tradeFile = "test_trade.csv"
quoteFile = "test_quote.csv"
resultDF = GroupAsofMain(tradeFile, quoteFile)
print(resultDF)
print("DF len " , len(resultDF))

ref_trades = pl.read_csv(tradeFile)
ref_quotes = pl.read_csv(quoteFile)
ref_results = ref_trades.join_asof(ref_quotes, on = "time", by = "symbol")
print((ref_results['bsize'] - resultDF['bsize']).drop_nulls().sum())
