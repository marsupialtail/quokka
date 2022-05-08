from optimizer import *
import pyarrow

sql = SQLContext()

sql.register_table("lineitem", "da", {"l_orderkey": pyarrow.uint64(), 
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
"l_comment":pyarrow.string()}, table_type="parquet")

sql.register_table("orders", "da", {
    "o_orderkey": pyarrow.uint64(),
    "o_custkey": pyarrow.uint64(),
    "o_orderstatus": pyarrow.string(),
    "o_totalprice": pyarrow.float64(),
    "o_orderdate": pyarrow.date32(),
    "o_orderpriority": pyarrow.string(),
    "o_clerk": pyarrow.string(),
    "o_shippriority": pyarrow.int32(),
    "o_comment": pyarrow.string()
}, table_type = "parquet")

sql.register_table("customer", "da",{
    "c_custkey": pyarrow.uint64(),
    "c_name": pyarrow.string(),
    "c_address": pyarrow.string(),
    "c_nationkey": pyarrow.uint64(),
    "c_phone": pyarrow.string(),
    "c_acctbal": pyarrow.float64(),
    "c_mktsegment": pyarrow.string(),
    "c_comment": pyarrow.string()
}, table_type = "parquet")

sql.register_table("part", "da", {
    "p_partkey" : pyarrow.uint64(),
    "p_name": pyarrow.string(),
    "p_mfgr": pyarrow.string(),
    "p_brand": pyarrow.string(),
    "p_type": pyarrow.string(),
    "p_size": pyarrow.int32(),
    "p_container": pyarrow.string(),
    "p_retailprice": pyarrow.float64(),
    "p_comment": pyarrow.string()
}, table_type = "parquet")

sql.register_table("supplier", "da", {
    "s_suppkey": pyarrow.uint64(),
    "s_name": pyarrow.string(),
    "s_address": pyarrow.string(),
    "s_nationkey": pyarrow.uint64(),
    "s_phone": pyarrow.string(),
    "s_acctbal": pyarrow.float64(),
    "s_comment": pyarrow.string()
}, table_type= "parquet")

sql.register_table("partsupp", "da", {
    "ps_partkey": pyarrow.uint64(),
    "ps_suppkey": pyarrow.uint64(),
    "ps_availqty": pyarrow.int32(),
    "ps_supplycost": pyarrow.float64(),
    "ps_comment": pyarrow.string()
}, table_type= "parquet")

sql.register_table("nation", "da",{
    "n_nationkey": pyarrow.uint64(),
    "n_name": pyarrow.string(),
    "n_regionkey": pyarrow.uint64(),
    "n_comment": pyarrow.string()
}, table_type="parquet")

sql.register_table("region", "da", {
    "r_regionkey" : pyarrow.uint64(),
    "r_name": pyarrow.string(),
    "r_comment": pyarrow.string()
}, table_type="parquet")

query5 = """
select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'EUROPE'
        and o_orderdate >= date '1994-01-01'
        and o_orderdate < date '1995-01-01'
group by
        n_name
order by
        revenue desc;
""" # modified final predicate to remove the addition of interval 1 year

query12 = """
select
        l_shipmode,
        sum(case
                when o_orderpriority = '1-URGENT'
                        or o_orderpriority = '2-HIGH'
                        then 1
                else 0
        end) as high_line_count,
        sum(case
                when o_orderpriority <> '1-URGENT'
                        and o_orderpriority <> '2-HIGH'
                        then 1
                else 0
        end) as low_line_count
from
        orders,
        lineitem
where
        o_orderkey = l_orderkey
        and l_shipmode in (':1', ':2')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and l_receiptdate >= date '1994-01-01'
        and l_receiptdate < date '1995-01-01'
group by
        l_shipmode
order by
        l_shipmode;
""" # modified final predicate to remove the addition of interval 1 year

query3 = """
select
        l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        o_orderdate,
        o_shippriority
from
        customer,
        orders,
        lineitem
where
        c_mktsegment = 'BUILDING'
        and c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate < date '1995-03-15'
        and l_shipdate > date '1995-03-15'
group by
        l_orderkey,
        o_orderdate,
        o_shippriority
order by
        revenue desc,
        o_orderdate
limit 10
"""

sql.get_graph_from_sql(query12)