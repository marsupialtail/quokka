from itertools import groupby
from api import * 
qc = QuokkaContext()
import pyarrow

lineitem = qc.read_parquet("test", list({"l_orderkey": pyarrow.uint64(), 
    "l_partkey": pyarrow.uint64(),
    "l_suppkey": pyarrow.uint64(),
    "l_linenumber": pyarrow.uint64(),
    "l_quantity": pyarrow.float64(),
    "l_extendedprice": pyarrow.float64(),
    "l_discount": pyarrow.float64(),
    "l_tax": pyarrow.float64(),
    "l_returnflag": pyarrow.string(),
    "l_linestatus": pyarrow.string(),
    "l_shipdate": pyarrow.date32(),
    "l_commitdate":pyarrow.date32(),
    "l_receiptdate":pyarrow.date32(),
    "l_shipinstruct":pyarrow.string(),
    "l_shipmode":pyarrow.string(),
    "l_comment":pyarrow.string()
}.keys()))
orders = qc.read_parquet("test2",list({
    "o_orderkey": pyarrow.uint64(),
    "o_custkey": pyarrow.uint64(),
    "o_orderstatus": pyarrow.string(),
    "o_totalprice": pyarrow.float64(),
    "o_orderdate": pyarrow.date32(),
    "o_orderpriority": pyarrow.string(),
    "o_clerk": pyarrow.string(),
    "o_shippriority": pyarrow.int32(),
    "o_comment": pyarrow.string()
}.keys()))
customer = qc.read_parquet("test3",list({
    "c_custkey": pyarrow.uint64(),
    "c_name": pyarrow.string(),
    "c_address": pyarrow.string(),
    "c_nationkey": pyarrow.uint64(),
    "c_phone": pyarrow.string(),
    "c_acctbal": pyarrow.float64(),
    "c_mktsegment": pyarrow.string(),
    "c_comment": pyarrow.string()
}.keys()))

part = qc.read_csv("part", list({
    "p_partkey" : pyarrow.uint64(),
    "p_name": pyarrow.string(),
    "p_mfgr": pyarrow.string(),
    "p_brand": pyarrow.string(),
    "p_type": pyarrow.string(),
    "p_size": pyarrow.int32(),
    "p_container": pyarrow.string(),
    "p_retailprice": pyarrow.float64(),
    "p_comment": pyarrow.string()
}.keys()))

supplier = qc.read_csv("supplier", list({
    "s_suppkey": pyarrow.uint64(),
    "s_name": pyarrow.string(),
    "s_address": pyarrow.string(),
    "s_nationkey": pyarrow.uint64(),
    "s_phone": pyarrow.string(),
    "s_acctbal": pyarrow.float64(),
    "s_comment": pyarrow.string()
}.keys()))

partsupp = qc.read_csv("partsupp", list({
    "ps_partkey": pyarrow.uint64(),
    "ps_suppkey": pyarrow.uint64(),
    "ps_availqty": pyarrow.int32(),
    "ps_supplycost": pyarrow.float64(),
    "ps_comment": pyarrow.string()
}.keys()))

nation = qc.read_csv("nation", list({
    "n_nationkey": pyarrow.uint64(),
    "n_name": pyarrow.string(),
    "n_regionkey": pyarrow.uint64(),
    "n_comment": pyarrow.string()
}.keys()))

region = qc.read_csv("region", list({
    "r_regionkey" : pyarrow.uint64(),
    "r_name": pyarrow.string(),
    "r_comment": pyarrow.string()
}.keys()))

testa = qc.read_csv("test",list({
    "key" : pyarrow.uint64(),
    "val1": pyarrow.uint64(),
    "val2": pyarrow.uint64(),
    "val3": pyarrow.uint64(),
}.keys()))

testb = qc.read_csv("test",list({
    "key" : pyarrow.uint64(),
    "val1": pyarrow.uint64(),
    "val2": pyarrow.uint64(),
    "val3": pyarrow.uint64()
}.keys()))


def do_1():

    '''
    Filters can be specified in SQL syntax. The columns in the SQL expression must exist in the schema
    '''

    d = lineitem.filter("l_shipdate <= date '1998-12-01' - interval '90' day")
    d = d.with_column("disc_price", lambda x: x["l_extendedprice"] * (1 - x["l_discount"]), required_columns ={"l_extendedprice", "l_discount"})
    d = d.with_column("charge", lambda x: x["l_extendedprice"] * (1 - x["l_discount"]) * (1 + x["l_tax"]), required_columns={"l_extendedprice", "l_discount", "l_tax"})

    f = d.groupby(["l_returnflag", "l_linestatus"], orderby=["l_returnflag", "l_linestatus"]).agg({"l_quantity":["sum","avg"], "l_extendedprice":["sum","avg"], "disc_price":"sum", 
        "charge":"sum", "l_discount":"avg","*":"count"})

def do_3():
    d = lineitem.join(orders,left_on="l_orderkey", right_on="o_orderkey")
    d = customer.join(d,left_on="c_custkey", right_on="o_custkey")
    d = d.filter("c_mktsegment = 'BUILDING' and o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15'")
    d = d.with_column("revenue", lambda x: x["l_extendedprice"] * ( 1 - x["l_discount"]) , required_columns={"l_extendedprice", "l_discount"})

    f = d.groupby(["l_orderkey","o_orderdate","o_shippriority"], orderby={'revenue':'desc','o_orderdate':'asc'}).agg({"revenue":["sum"]})

def do_4():

    '''
    The DataFrame API does not (and does not plan to) do things like nested subquery decorrelation and common subexpression elimination.
    You should either do that yourself, or use the upcoming SQL API and rely on the decorrelation by SQLGlot.
    '''

    d = lineitem.filter("l_commitdate < l_receiptdate")
    d = d.distinct("l_orderkey")
    d = d.join(orders, left_on="l_orderkey", right_on="o_orderkey")
    d = d.filter("o_orderdate >= date '1993-07-01' and o_orderdate < date '1993-10-01'")
    f = d.groupby("o_orderpriority").agg({'*':['count']})


def do_5():

    '''
    Quokka currently does not pick the best join order, or the best join strategy. This is upcoming improvement for a future release.
    You will have to pick the best join order. One way to do this is to do sparksql.explain and "borrow" Spark Catalyst CBO's plan.
    As a general rule of thumb you want to join small tables first and then bigger ones.
    '''

    d = customer.join(nation, left_on="c_nationkey", right_on="n_nationkey")
    d = d.join(orders, left_on="c_custkey", right_on="o_custkey")
    d = d.join(lineitem, left_on="o_orderkey", right_on="l_orderkey")
    d = d.join(supplier, left_on="l_suppkey", right_on="s_suppkey")
    d = d.filter("s_nationkey = c_nationkey and n_regionkey = 2 and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year")
    d = d.with_column("revenue", lambda x: x["l_extendedprice"] * ( 1 - x["l_discount"]) , required_columns={"l_extendedprice", "l_discount"})
    f = d.groupby("n_name", orderby={"revenue":'desc'}).agg({"revenue":["sum"]})

def do_6():
    d = lineitem.filter("l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 24")
    d = d.with_column("revenue", lambda x: x["l_extendedprice"] * ( 1 - x["l_discount"]), required_columns={"l_extendedprice", "l_discount"})
    f = d.aggregate({"revenue":["sum"]})

def do_12():

    d = lineitem.join(orders,left_on="l_orderkey", right_on="o_orderkey")
    
    d = d.filter("l_shipmode IN ('MAIL','SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and \
        l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1995-01-01'")

    d = d.with_column("high", lambda x: (x["o_orderpriority"] == "1-URGENT") | (x["o_orderpriority"] == "2-HIGH"), required_columns={"o_orderpriority"})
    d = d.with_column("low", lambda x: (x["o_orderpriority"] != "1-URGENT") & (x["o_orderpriority"] != "2-HIGH"), required_columns={"o_orderpriority"})

    # you could also write:
    # d = d.with_column("high", lambda x: x.apply(lambda x:((x["o_orderpriority"] == "1-URGENT") | (x["o_orderpriority"] == "2-HIGH")).astype(int), axis=1), required_columns={"o_orderpriority"}, engine = "pandas")
    
    f = d.groupby("l_shipmode").aggregate(aggregations={'high':['sum'], 'low':['sum']})

def word_count():

    def udf2(x):
        da = compute.list_flatten(compute.ascii_split_whitespace(x["text"]))
        c = da.value_counts().flatten()
        return pa.Table.from_arrays([c[0], c[1]], names=["word","count"]).to_pandas()
    
    words = qc.read_csv("s3://wordcount-input",["text"],sep="|")
    counted = words.transform( udf2, new_schema = ["word", "count"], required_columns = None, foldable=True)
    counted.aggregate(aggregations={})

def test1():
    d = lineitem.join(orders,left_on="l_orderkey", right_on="o_orderkey")
    d = customer.join(d,left_on="c_custkey", right_on="o_custkey")

    d = d.filter("l_commitdate < l_receiptdate")
    d = d.filter("l_commitdate > l_shipdate")
    d = d.filter("o_clerk > o_comment")
    d = d.filter("l_tax >o_totalprice")

    d = d.select(["l_commitdate", "o_clerk", "c_name"])
    d.collect()

def self_join_test():
    
    f = lineitem.join(lineitem,on="l_orderkey",suffix="_2")
    d = f.filter("l_commitdate < l_receiptdate")

    d.collect()


def suffix_test():
    d = join(testa,testb, on="key")
    d = d.filter(d.val1 > d.val1_2)
    d = d.filter(d.val2_2 > d.val3_2)
    d = d.filter(d.val1_2 == d.val3_2)
    d = d.filter(d.val1 > d.val2)
    d = d.compute()
    sql.__push_filter__(d)
    d.walk()

do_3()
#do_12()
#test1()