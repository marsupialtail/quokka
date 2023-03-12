import pandas as pd
import polars as pl
import math
import numpy as np
import random
from datetime import datetime
from polars.internals.dataframe.frame import DataFrame

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

def asofQuokka(batch: DataFrame, indicator: int):
    # print("indicator", indicator)
    global trade_dict
    global quote_dict
    global result
    if indicator == 0:
        if trade_dict is None:
            trade_dict = batch
        else:
            trade_dict.vstack(batch, in_place = True)
    else:
        if quote_dict is None:
            quote_dict = batch
        else:
            quote_dict.vstack(batch, in_place = True)

    if trade_dict is None or quote_dict is None or len(trade_dict) == 0 or len(quote_dict) == 0:
        return

    joinable_trades = trade_dict.filter(trade_dict["time"] < quote_dict["time"][-1])
    if len(joinable_trades) == 0:
        return
    remaining_trades =  trade_dict.filter(trade_dict["time"] >= quote_dict["time"][-1])
    joinable_quotes = quote_dict.filter(quote_dict["time"] <= joinable_trades["time"][-1])
    remaining_quotes = quote_dict.filter(quote_dict["time"] >= joinable_quotes["time"][-1])

    this_result = joinable_trades.join_asof(joinable_quotes, on = "time", by = "symbol")
    if result is None:
        result = this_result
    else:
        result.vstack(this_result, in_place = True)
    
    trade_dict = remaining_trades
    quote_dict = remaining_quotes


def getBatch(data, total_rows, batch_head, batch_tail):
    if batch_tail < total_rows:
        batch = data[batch_head:batch_tail]
    else:
        batch = data[batch_head:total_rows]
    return batch

    
def GroupAsofMain(tradeFile, quoteFile):
    trades = pl.read_csv(tradeFile, has_header=True, sep = ",")
    quotes = pl.read_csv(quoteFile, has_header=True, sep = ",")
    trade_rows = len(trades) 
    quote_rows = len(quotes)
    batch_size = 500
    trade_batches = math.ceil(trade_rows/batch_size)
    quote_batches = math.ceil(quote_rows/batch_size)
    counter0, counter1 = 0, 0
    trade_head, trade_tail, quote_head, quote_tail = 0,0,0,0
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
        asofQuokka(batch, indicator)
    

    result.vstack(trade_dict.join_asof(quote_dict, on = "time", by = "symbol"), in_place = True)


    # print("UNMATCHED TRADES", len(unmatched_trades))
    return result


tradeFile = "test_trade.csv"
quoteFile = "test_quote.csv"
resultDF = GroupAsofMain(tradeFile, quoteFile)
print(resultDF)
print("DF len " , len(resultDF))
print(len(trade_dict), len(quote_dict))

ref_trades = pl.read_csv(tradeFile)
ref_quotes = pl.read_csv(quoteFile)
ref_results = ref_trades.join_asof(ref_quotes, on = "time", by = "symbol")
print((ref_results['bsize'] - resultDF['bsize']).drop_nulls().sum())
