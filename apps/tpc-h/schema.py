lineitem_scheme = ["l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice", 
"l_discount","l_tax","l_returnflag","l_linestatus","l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct",
"l_shipmode","l_comment", "null"]
order_scheme = ["o_orderkey", "o_custkey","o_orderstatus","o_totalprice","o_orderdate","o_orderpriority","o_clerk",
"o_shippriority","o_comment", "null"]
customer_scheme = ["c_custkey","c_name","c_address","c_nationkey","c_phone","c_acctbal","c_mktsegment","c_comment", "null"]
part_scheme = ["p_partkey","p_name","p_mfgr","p_brand","p_type","p_size","p_container","p_retailprice","p_comment","null"]
supplier_scheme = ["s_suppkey","s_name","s_address","s_nationkey","s_phone","s_acctbal","s_comment","null"]
partsupp_scheme = ["ps_partkey","ps_suppkey","ps_availqty","ps_supplycost","ps_comment","null"]
nation_scheme = ["n_nationkey","n_name","n_regionkey","n_comment","null"]
region_scheme = ["r_regionkey" ,"r_name","r_comment","null"]
schema_quotes = ["time","symbol","seq","bid","ask","bsize","asize","is_nbbo"]
schema_trades = ["time","symbol","size","price"]

