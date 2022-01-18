import dask.dataframe as dd
import time
import sys
'''
start = time.time()
df = dd.read_csv("s3://yugan/a-big.csv")
df.head(1)
print(time.time()-start)
start = time.time()
df1 = dd.read_csv("s3://yugan/b-big.csv")
df1.head(1)
print(time.time()-start)
start = time.time()
df.merge(df1,on="key",how="inner").compute()
print(time.time()-start)

start = time.time()
df = dd.read_csv("s3://yugan/a-big.csv")
df1 = dd.read_csv("s3://yugan/b-big.csv")
result = df.merge(df1,on="key",how="inner").compute()
result.to_parquet("s3://yugan/dask-result.parquet")
print(time.time()-start)
'''
def do_12():
    lineitem_scheme = ["l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice", 
    "l_discount","l_tax","l_returnflag","l_linestatus","l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct",
    "l_shipmode","l_comment"]
    order_scheme = ["o_orderkey", "o_custkey","o_orderstatus","o_totalprice","o_orderdate","o_orderpriority","o_clerk",
    "o_shippriority","o_comment"]
    start = time.time()
    orders = dd.read_csv("s3://tpc-h-small/orders.tbl",sep="|",header = 0)
    lineitem = dd.read_csv("s3://tpc-h-small/lineitem.tbl",sep="|", header = 0)
    orders = orders.rename(columns=dict(zip(orders.columns, order_scheme)))
    lineitem = lineitem.rename(columns=dict(zip(lineitem.columns, lineitem_scheme)))
    
    filtered_lineitem = lineitem.loc[((lineitem.l_shipmode == "MAIL") | (lineitem.l_shipmode == "SHIP")) & (lineitem.l_commitdate < lineitem.l_receiptdate) & (lineitem.l_shipdate < lineitem.l_commitdate) & (lineitem.l_receiptdate >= "1994-01-01") & (lineitem.l_receiptdate < "1995-01-01")][["l_orderkey","l_shipmode"]]
    
    filtered_order = orders[["o_orderkey","o_orderpriority"]]
    
    result = filtered_lineitem.merge(filtered_order,right_on="o_orderkey", left_on="l_orderkey")[["l_shipmode","o_orderpriority"]]
    
    result["high"] = ((result["o_orderpriority"] == "1-URGENT") | (result["o_orderpriority"] == "2-HIGH")).astype(int)
    result["low"] = ((result["o_orderpriority"] != "1-URGENT") & (result["o_orderpriority"] != "2-HIGH")).astype(int)
    print(result.groupby('l_shipmode').agg({'high':'sum','low':'sum'}).compute())
    
    print(time.time() - start)

if int(sys.argv[1]) == 12:
    do_12()
