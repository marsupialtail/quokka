from pyquokka.df import * 
from pyquokka.utils import LocalCluster, QuokkaClusterManager
from schema import * 
mode = "S3"
format = "parquet"
disk_path = "/home/ziheng/tpc-h/"
#disk_path = "s3://yugan/tpc-h-out/"
s3_path_csv = "s3://tpc-h-csv/"
s3_path_parquet = "s3://tpc-h-sf100-parquet/"

import pyarrow as pa
import pyarrow.compute as compute
import polars
polars.Config().set_tbl_cols(10)

if mode == "DISK":
    cluster = LocalCluster()
elif mode == "S3":
    manager = QuokkaClusterManager(key_name = "zihengw", key_location = "/home/ziheng/Downloads/zihengw.pem")
    manager.start_cluster("16.json")
    cluster = manager.get_cluster_from_json("16.json")
else:
    raise Exception

qc = QuokkaContext(cluster,2, 1)
qc.set_config("fault_tolerance", True)
qc.set_config("blocking", False)

if mode == "DISK":
    if format == "csv":
        lineitem = qc.read_csv(disk_path + "lineitem.tbl", sep="|", has_header=True)
        orders = qc.read_csv(disk_path + "orders.tbl", sep="|", has_header=True)
        customer = qc.read_csv(disk_path + "customer.tbl",sep = "|", has_header=True)
        part = qc.read_csv(disk_path + "part.tbl", sep = "|", has_header=True)
        supplier = qc.read_csv(disk_path + "supplier.tbl", sep = "|", has_header=True)
        partsupp = qc.read_csv(disk_path + "partsupp.tbl", sep = "|", has_header=True)
        nation = qc.read_csv(disk_path + "nation.tbl", sep = "|", has_header=True)
        region = qc.read_csv(disk_path + "region.tbl", sep = "|", has_header=True)
    elif format == "parquet":
        lineitem = qc.read_parquet(disk_path + "lineitem.parquet")
        orders = qc.read_parquet(disk_path + "orders.parquet")
        customer = qc.read_parquet(disk_path + "customer.parquet")
        part = qc.read_parquet(disk_path + "part.parquet")
        supplier = qc.read_parquet(disk_path + "supplier.parquet")
        partsupp = qc.read_parquet(disk_path + "partsupp.parquet")
        nation = qc.read_parquet(disk_path + "nation.parquet")
        region = qc.read_parquet(disk_path + "region.parquet")
    else:
        raise Exception
elif mode == "S3":
    if format == "csv":
        lineitem = qc.read_csv(s3_path_csv + "lineitem/lineitem.tbl.1", lineitem_scheme, sep="|").drop(["null"])
        orders = qc.read_csv(s3_path_csv + "orders/orders.tbl.1", order_scheme, sep="|").drop(["null"])
        customer = qc.read_csv(s3_path_csv + "customer/customer.tbl.1",customer_scheme, sep = "|").drop(["null"])
        part = qc.read_csv(s3_path_csv + "part/part.tbl.1", part_scheme, sep = "|").drop(["null"])
        supplier = qc.read_csv(s3_path_csv + "supplier/supplier.tbl.1", supplier_scheme, sep = "|").drop(["null"])
        partsupp = qc.read_csv(s3_path_csv + "partsupp/partsupp.tbl.1", partsupp_scheme, sep = "|").drop(["null"])
        nation = qc.read_csv(s3_path_csv + "nation/nation.tbl", nation_scheme, sep = "|").drop(["null"])
        region = qc.read_csv(s3_path_csv + "region/region.tbl", region_scheme, sep = "|").drop(["null"])
    elif format == "parquet":
        lineitem = qc.read_parquet(s3_path_parquet + "lineitem.parquet/*")
        orders = qc.read_parquet(s3_path_parquet + "orders.parquet/*")
        customer = qc.read_parquet(s3_path_parquet + "customer.parquet/*")
        part = qc.read_parquet(s3_path_parquet + "part.parquet/*") 
        supplier = qc.read_parquet(s3_path_parquet + "supplier.parquet/*")
        partsupp = qc.read_parquet(s3_path_parquet + "partsupp.parquet/*")
        nation = qc.read_parquet(s3_path_parquet + "nation.parquet/*")
        region = qc.read_parquet(s3_path_parquet + "region.parquet/*")

    else:
        raise Exception
else:
    raise Exception


def do_1():

    d = lineitem.filter_sql("l_shipdate <= date '1998-12-01' - interval '90' day")
    d = d.with_columns({"disc_price": d["l_extendedprice"] * (1 - d["l_discount"]), 
                        "charge": d["l_extendedprice"] * (1 - d["l_discount"]) * (1 + d["l_tax"])})
    
    f = d.groupby(["l_returnflag", "l_linestatus"], orderby=["l_returnflag","l_linestatus"]).agg({"l_quantity":["sum","avg"], "l_extendedprice":["sum","avg"], "disc_price":"sum", 
        "charge":"sum", "l_discount":"avg","*":"count"})
    return f.collect()

def do_1_1():

    d = lineitem.filter_sql("l_shipdate <= date '1998-12-01' - interval '90' day")
    d = d.with_columns({"disc_price": lambda x: x["l_extendedprice"] * (1 - x["l_discount"]), 
                        "charge": d["l_extendedprice"] * (1 - d["l_discount"]) * (1 + d["l_tax"])}, 
                        required_columns ={"l_extendedprice", "l_discount"})
    
    f = d.groupby(["l_returnflag", "l_linestatus"], orderby=["l_returnflag","l_linestatus"]).agg({"l_quantity":["sum","avg"], "l_extendedprice":["sum","avg"], "disc_price":"sum", 
        "charge":"sum", "l_discount":"avg","*":"count"})
    return f.collect()

def do_1_2():

    d = lineitem.filter_sql("l_shipdate <= date '1998-12-01' - interval '90' day")
    d = d.with_columns_sql("l_extendedprice * (1 - l_discount) as disc_price, l_extendedprice * (1 - l_discount) * (1 + l_tax) as charge")
    
    f = d.groupby(["l_returnflag", "l_linestatus"], orderby=["l_returnflag","l_linestatus"]).agg({"l_quantity":["sum","avg"], "l_extendedprice":["sum","avg"], "disc_price":"sum", 
        "charge":"sum", "l_discount":"avg","*":"count"})
    return f.collect()

def do_1_sql():

    d = lineitem.filter_sql("l_shipdate <= date '1998-12-01' - interval '90' day")
    f = d.groupby(["l_returnflag", "l_linestatus"]).agg_sql( 
                             """sum(l_quantity) as sum_qty,
                                sum(l_extendedprice) as sum_base_price,
                                sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
                                sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
                                avg(l_quantity) as avg_qty,
                                avg(l_extendedprice) as avg_price,
                                avg(l_discount) as avg_disc,
                                count(*) as count_order"""
    )
    f.explain()
    return f.collect()
                             
def do_2():
    '''
    Quokka does not do query unnesting.
    '''
    qc.set_config("optimize_joins", False)
    europe = region.filter_sql("r_name = 'EUROPE'")
    european_nations = nation.join(europe, left_on="n_regionkey",right_on="r_regionkey").select(["n_name","n_nationkey"])
    d = supplier.join(european_nations, left_on="s_nationkey", right_on="n_nationkey")
    d = partsupp.join(d, left_on="ps_suppkey", right_on="s_suppkey")
    f = d.groupby("ps_partkey").aggregate({"ps_supplycost":"min"}).rename({"ps_supplycost_min":"min_cost","ps_partkey":"europe_key"})

    k = f.join(part, left_on="europe_key", right_on="p_partkey", suffix="_3")
    d = qc.read_parquet(s3_path_parquet + "supplier.parquet/*").join(european_nations, left_on="s_nationkey", right_on="n_nationkey")
    d = qc.read_parquet(s3_path_parquet + "partsupp.parquet/*").join(d, left_on="ps_suppkey", right_on="s_suppkey")
    d = d.join(k, left_on="ps_supplycost", right_on="min_cost", suffix="_2")
    d = d.filter_sql("""europe_key = ps_partkey and p_size = 15 and p_type like '%BRASS' """)
    d = d.select(["s_acctbal", "s_name", "n_name", "europe_key", "p_mfgr", "s_address", "s_phone", "s_comment"])

    f = d.top_k(["s_acctbal","n_name","s_name","europe_key"],100, descending=[True,False,False,False])
    f.explain()
    result = f.collect()
    qc.set_config("optimize_joins", True)
    return result


def do_3():
    d = lineitem.join(orders,left_on="l_orderkey", right_on="o_orderkey")
    d = customer.join(d,left_on="c_custkey", right_on="o_custkey")
    d = d.filter_sql("c_mktsegment = 'BUILDING' and o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15'")
    d = d.with_column("revenue", lambda x: x["l_extendedprice"] * ( 1 - x["l_discount"]) , required_columns={"l_extendedprice", "l_discount"})

    #f = d.groupby(["l_orderkey","o_orderdate","o_shippriority"], orderby=[('revenue','desc'),('o_orderdate','asc')]).agg({"revenue":["sum"]})
    f = d.groupby(["l_orderkey","o_orderdate","o_shippriority"]).agg({"revenue":["sum"]})
    f.explain()
    return f.collect()

# def do_3_sql():
#     qc.set_config("optimize_joins", False)
#     d = customer.join(orders,left_on="c_custkey", right_on="o_custkey")
#     d = d.join(lineitem,left_on="o_orderkey", right_on="l_orderkey")
#     d = d.filter_sql("c_mktsegment = 'BUILDING' and o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15'")
#     d = d.groupby(["o_orderkey","o_orderdate","o_shippriority"]).agg_sql("sum(l_extendedprice * (1 - l_discount)) as revenue")
#     f = d.top_k(["revenue", "o_orderdate"], 10, descending=[True, False])
#     f.explain()
#     return f.collect()

def do_3_sql():
    d = lineitem.join(orders,left_on="l_orderkey", right_on="o_orderkey")
    d = customer.join(d,left_on="c_custkey", right_on="o_custkey")
    d = d.filter_sql("c_mktsegment = 'BUILDING' and o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15'")
    d = d.groupby(["l_orderkey","o_orderdate","o_shippriority"]).agg_sql("sum(l_extendedprice * (1 - l_discount)) as revenue")
    f = d.top_k(["revenue", "o_orderdate"], 10, descending=[True, False])
    f.explain()
    return f.collect()

def do_4():

    '''
    The DataFrame API does not (and does not plan to) do things like nested subquery decorrelation and common subexpression elimination.
    You should either do that yourself, or use the upcoming SQL API and rely on the decorrelation by SQLGlot.
    '''

    d = lineitem.filter_sql("l_commitdate < l_receiptdate")
    #d = d.distinct("l_orderkey")
    d = orders.join(d, left_on="o_orderkey", right_on="l_orderkey", how = "semi")
    d = d.filter_sql("o_orderdate >= date '1993-07-01' and o_orderdate < date '1993-10-01'")
    f = d.groupby("o_orderpriority").agg({'*':['count']})
    f.explain()
    return f.collect()


def do_4_sql():
    d = lineitem.filter_sql("l_commitdate < l_receiptdate")
    d = orders.join(d, left_on="o_orderkey", right_on="l_orderkey", how = "semi")
    d = d.filter_sql("o_orderdate >= date '1993-07-01' and o_orderdate < date '1993-10-01'")
    f = d.groupby("o_orderpriority").agg_sql("count(*) as count_order")
    f.explain()
    return f.collect()

def do_5():

    '''
    Quokka currently does not pick the best join order, or the best join strategy. This is upcoming improvement for a future release.
    You will have to pick the best join order. One way to do this is to do sparksql.explain and "borrow" Spark Catalyst CBO's plan.
    As a general rule of thumb you want to join small tables first and then bigger ones.
    '''

    asia = region.filter_sql("r_name == 'ASIA'")
    asian_nations = nation.join(asia, left_on="n_regionkey",right_on="r_regionkey").select(["n_name","n_nationkey"])
    d = customer.join(asian_nations, left_on="c_nationkey", right_on="n_nationkey")
    d = d.join(orders, left_on="c_custkey", right_on="o_custkey", suffix="_3")
    d = d.join(lineitem, left_on="o_orderkey", right_on="l_orderkey", suffix="_4")
    d = d.join(supplier, left_on="l_suppkey", right_on="s_suppkey", suffix="_5")
    d = d.filter_sql("s_nationkey = c_nationkey and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year")
    d = d.with_column("revenue", lambda x: x["l_extendedprice"] * ( 1 - x["l_discount"]) , required_columns={"l_extendedprice", "l_discount"})
    #f = d.groupby("n_name", orderby=[("revenue",'desc')]).agg({"revenue":["sum"]})
    f = d.groupby("n_name").agg({"revenue":["sum"]})
    f.explain()
    return f.collect().sort("revenue", descending = True)


def do_5_sql():
    qc.set_config("optimize_joins", False)
    asia = region.filter_sql("r_name == 'ASIA'")
    asian_nations = nation.join(asia, left_on="n_regionkey",right_on="r_regionkey").select(["n_name","n_nationkey"])
    d = customer.join(asian_nations, left_on="c_nationkey", right_on="n_nationkey")
    d = d.join(orders, left_on="c_custkey", right_on="o_custkey", suffix="_3")
    d = d.join(lineitem, left_on="o_orderkey", right_on="l_orderkey", suffix="_4")
    d = d.join(supplier, left_on="l_suppkey", right_on="s_suppkey", suffix="_5")
    d = d.filter_sql("s_nationkey = c_nationkey and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year")
    f = d.groupby("n_name").agg_sql("sum(l_extendedprice * (1 - l_discount)) as revenue")
    f.explain()
    result = f.collect().sort("revenue", descending = True)
    qc.set_config("optimize_joins", True)
    return result

def do_6():
    d = lineitem.filter_sql("l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 24")
    d = d.with_column("revenue", lambda x: x["l_extendedprice"] * x["l_discount"], required_columns={"l_extendedprice", "l_discount"})
    f = d.aggregate({"revenue":["sum"]})
    return f.collect()

def do_6_sql():
    d = lineitem.filter_sql("l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 24")
    f = d.agg_sql("sum(l_extendedprice * l_discount) as revenue")
    return f.collect()

def do_7():
    d1 = customer.join(nation, left_on = "c_nationkey", right_on = "n_nationkey")
    d1 = d1.join(orders, left_on = "c_custkey", right_on = "o_custkey", suffix = "_3")
    d2 = supplier.join(nation, left_on="s_nationkey", right_on = "n_nationkey")
    d2 = lineitem.join(d2, left_on = "l_suppkey", right_on = "s_suppkey", suffix = "_3")
    
    d = d1.join(d2, left_on = "o_orderkey", right_on = "l_orderkey",suffix="_4")
    d = d.rename({"n_name_4": "supp_nation", "n_name": "cust_nation"})
    d = d.filter_sql("""(
                                (supp_nation = 'FRANCE' and cust_nation = 'GERMANY')
                                or (supp_nation = 'GERMANY' and cust_nation = 'FRANCE')
                        )
                        and l_shipdate between date '1995-01-01' and date '1996-12-31'""")
    d = d.with_column("l_year", lambda x: x["l_shipdate"].dt.year(), required_columns = {"l_shipdate"})
    d = d.with_column("volume", lambda x: x["l_extendedprice"] * ( 1 - x["l_discount"]) , required_columns={"l_extendedprice", "l_discount"})
    f = d.groupby(["supp_nation","cust_nation","l_year"], orderby=["supp_nation","cust_nation","l_year"]).aggregate({"volume":"sum"})
    f.explain()
    return f.collect().sort(["supp_nation","cust_nation","l_year"])

def do_7_sql():
    d1 = customer.join(nation, left_on = "c_nationkey", right_on = "n_nationkey")
    d1 = d1.join(orders, left_on = "c_custkey", right_on = "o_custkey", suffix = "_3")
    d2 = supplier.join(nation, left_on="s_nationkey", right_on = "n_nationkey")
    d2 = lineitem.join(d2, left_on = "l_suppkey", right_on = "s_suppkey", suffix = "_3")
    
    d = d1.join(d2, left_on = "o_orderkey", right_on = "l_orderkey",suffix="_4")
    d = d.rename({"n_name_4": "supp_nation", "n_name": "cust_nation"})
    d = d.filter_sql("""(
                                (supp_nation = 'FRANCE' and cust_nation = 'GERMANY')
                                or (supp_nation = 'GERMANY' and cust_nation = 'FRANCE')
                        )
                        and l_shipdate between date '1995-01-01' and date '1996-12-31'""")
    d = d.with_columns({"l_year": lambda x: x["l_shipdate"].dt.year()}, required_columns = {"l_shipdate"})
    f = d.groupby(["supp_nation","cust_nation","l_year"], orderby=["supp_nation","cust_nation","l_year"]).\
        agg_sql("sum(l_extendedprice * ( 1 - l_discount)) as volume")
    f.explain()
    return f.collect().sort(["supp_nation","cust_nation","l_year"])

def do_8():
    america = region.filter_sql("r_name = 'AMERICA'")
    american_nations = nation.join(america, left_on="n_regionkey",right_on="r_regionkey").select(["n_nationkey"])
    american_customers = customer.join(american_nations, left_on="c_nationkey", right_on="n_nationkey")
    american_orders = orders.join(american_customers, left_on = "o_custkey", right_on="c_custkey")
    d = lineitem.join(part, left_on="l_partkey", right_on="p_partkey")
    d = d.join(american_orders, left_on = "l_orderkey", right_on = "o_orderkey")
    d = d.join(supplier, left_on="l_suppkey", right_on="s_suppkey")
    d = d.join(nation, left_on="s_nationkey", right_on = "n_nationkey")
    d = d.filter_sql("""
       o_orderdate between date '1995-01-01' and date '1996-12-31'
        and p_type = 'ECONOMY ANODIZED STEEL'         
    """)
    d = d.with_columns({"o_year": lambda x: x["o_orderdate"].dt.year(), "volume": lambda x: x["l_extendedprice"] * (1 - x["l_discount"])}, required_columns = {"l_extendedprice", "l_discount", "o_orderdate"})
    d = d.rename({"n_name" : "nation"})
    d = d.with_columns({"brazil_volume": lambda x: x["volume"] * (x["nation"] == 'BRAZIL')}, required_columns={"volume", "nation"})
    f = d.groupby("o_year").aggregate(aggregations={"volume":"sum", "brazil_volume":"sum"})
    f.explain()
    result = f.collect().sort("o_year")
    return result

# join ordering will be hard for this one
def do_9():

    qc.set_config("optimize_joins", False)
    d = partsupp.join(part, left_on="ps_partkey", right_on="p_partkey")
    d1 = supplier.join(nation, left_on="s_nationkey", right_on="n_nationkey")
    d = d1.join(d, left_on="s_suppkey", right_on="ps_suppkey")
    d = d.join(lineitem, left_on="ps_partkey", right_on="l_partkey")
    d = d.filter_sql("s_suppkey = l_suppkey and p_name like '%green%'")
    d = d.join(orders, left_on = "l_orderkey", right_on = "o_orderkey")
    d = d.with_columns({"o_year": lambda x: x["o_orderdate"].dt.year(), 
                        "amount": d["l_extendedprice"] * (1 - d["l_discount"]) - d["ps_supplycost"] * d["l_quantity"]}, 
                        required_columns = {"o_orderdate"})
    d = d.rename({"n_name" : "nation"})
    f = d.groupby(["nation", "o_year"]).aggregate(aggregations = {"amount":"sum"})
    f.explain()
    result = f.collect()
    qc.set_config("optimize_joins", True)
    return result.sort(['nation', 'o_year'], descending=[False, True])

def do_10():
    d = customer.join(nation, left_on="c_nationkey", right_on="n_nationkey")
    d = d.join(orders, left_on = "c_custkey", right_on = "o_custkey")
    d = d.join(lineitem, left_on = "o_orderkey", right_on="l_orderkey")
    d = d.filter_sql("""
        o_orderdate >= date '1993-10-01'
        and o_orderdate < date '1993-10-01' + interval '3' month
        and l_returnflag = 'R'
    """)
    d = d.groupby(["c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", \
        "c_address", "c_comment"]).agg_sql("sum(l_extendedprice * (1 - l_discount)) as revenue")
    f = d.top_k("revenue", 20, descending=True)
    return f.collect()

def do_11():
    d = supplier.join(nation.filter_sql("n_name == 'GERMANY'"), left_on="s_nationkey", right_on="n_nationkey")
    d = d.join(partsupp, left_on="s_suppkey", right_on="ps_suppkey")
    d = d.with_columns({"value": d["ps_supplycost"] * d["ps_availqty"]})
    d = d.select(["ps_partkey", "value"]).compute()
    temp = qc.read_dataset(d)
    val = (temp.sum("value") * 0.0001)[0,0]
    return temp.groupby("ps_partkey").aggregate(aggregations={"value":"sum"}).filter_sql("value_sum > " + str(val)).collect()# .sort('value_sum',reverse=True)

def do_12():
    
    d = lineitem.join(orders,left_on="l_orderkey", right_on="o_orderkey")
    
    d = d.filter_sql("l_shipmode IN ('MAIL','SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and \
        l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1995-01-01'")

    d = d.with_columns({"high": (d["o_orderpriority"] == "1-URGENT") | (d["o_orderpriority"] == "2-HIGH"), 
                        "low": (d["o_orderpriority"] != "1-URGENT") & (d["o_orderpriority"] != "2-HIGH")})

    f = d.groupby("l_shipmode").aggregate(aggregations={'high':['sum'], 'low':['sum']})
    return f.collect()

def do_12_sql():

    d = lineitem.join(orders,left_on="l_orderkey", right_on="o_orderkey")

    d = d.filter_sql("l_shipmode IN ('MAIL','SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and \
        l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1995-01-01'")

    f = d.groupby("l_shipmode").agg_sql("""
        sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,
        sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
    """)
    return f.collect()

def do_13():

    d = customer.join(orders, left_on="c_custkey", right_on="o_custkey", how="left")
    d = d.filter_sql("o_comment not like '%special%requests%'")
    c_orders = d.groupby("c_custkey").agg_sql("count(o_orderkey) as c_count")
    result = c_orders.groupby("c_count").aggregate(aggregations={"*":"count"}).collect().sort('count')
    return result

def do_14():
    d = lineitem.join(part, left_on="l_partkey", right_on="p_partkey")
    d = d.filter_sql("l_shipdate >= date '1995-09-01' and l_shipdate < date '1995-09-01' + interval '1' month")
    f = d.agg_sql("""100.00 * sum(case
                when p_type like 'PROMO%'
                        then l_extendedprice * (1 - l_discount)
                else 0
        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue""")
    f.explain()
    return f.collect()

def do_15():

    # first compute the revenue
    d = lineitem.filter_sql("l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '3' month")
    d = d.with_columns({"revenue": lambda x: x["l_extendedprice"] * (1 - x["l_discount"])}, required_columns={"l_extendedprice", "l_discount"})
    revenue = d.groupby("l_suppkey").aggregate(aggregations={"revenue":"sum"})
    revenue = revenue.compute()
    print(revenue)
    revenue = qc.read_dataset(revenue)
    max_revenue = revenue.max("revenue_sum")[0,0]
    print(max_revenue)
    result = supplier.join(revenue, left_on="s_suppkey", right_on="l_suppkey").filter_sql("revenue_sum == " + str(max_revenue)).select(["s_suppkey", "s_name", "s_address", "s_phone", "revenue_sum"])
    result.explain()
    return result.collect()

def do_16():

    bad_suppliers = supplier.filter_sql("s_comment like '%Customer%Complaints%'")
    d = partsupp.join(bad_suppliers, left_on="ps_suppkey", right_on="s_suppkey", how="anti")
    d = d.join(part, left_on="ps_partkey", right_on="p_partkey", how="inner")
    d = d.filter_sql("p_brand != 'Brand#45' and p_type not like 'MEDIUM POLISHED%' and p_size in (49, 14, 23, 45, 19, 3, 36, 9)")
    result = d.groupby(["p_brand", "p_type", "p_size"]).count_distinct("ps_suppkey")
    result.explain()
    result = result.collect().sort(["ps_suppkey", "p_brand", "p_type", "p_size" ], descending = [True, False, False, False])
    return result

def do_17():
    u_0 = lineitem.groupby("l_partkey").agg_sql("0.2 * AVG(l_quantity) AS avg_quantity").compute()
    u_0 = qc.read_dataset(u_0)
    print(u_0)
    d = part.join(u_0, left_on="p_partkey", right_on="l_partkey", how="inner")
    d = d.join(lineitem, left_on="p_partkey", right_on="l_partkey", how="inner")
    d = d.filter_sql("p_brand = 'Brand#23' and p_container = 'MED BOX' and l_quantity < avg_quantity")
    f = d.agg_sql("SUM(l_extendedprice) / 7.0 AS avg_yearly")
    f.explain()
    result = f.collect()
    return result

def do_18():
    u_0 = lineitem.groupby("l_orderkey").agg_sql("SUM(l_quantity) AS sum_quant").filter_sql("sum_quant > 300").compute()
    u_0 = qc.read_dataset(u_0)
    d = customer.join(orders, left_on="c_custkey", right_on="o_custkey", how="inner")
    d = d.join(u_0, left_on="o_orderkey", right_on="l_orderkey", how="inner")
    d = d.join(lineitem, left_on="o_orderkey", right_on="l_orderkey", how="inner")
    d = d.groupby(["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice"]).agg_sql("sum(l_quantity) AS total_quantity")
    d = d.top_k(["o_totalprice", "o_orderdate"], 100, descending = [True, False])
    return d.collect()

def do_19():
    print("manual DNF unnesting currently needed for reasonable performance.")
    d = lineitem.filter_sql("""
            (l_quantity >= 1 and l_quantity <= 30)
            and (l_shipmode in ('AIR', 'AIR REG'))
            and (l_shipinstruct = 'DELIVER IN PERSON')
        """)\
        .join(part.filter_sql(
        """(p_brand = 'Brand#12' or p_brand = 'Brand#23' or p_brand = 'Brand#34')
            and (p_size between 1 and 15)
            and (p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG', 'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK', 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'))
        """), left_on="l_partkey", right_on="p_partkey", how="inner")
    d = d.filter_sql("""
                (
                    p_brand = 'Brand#12'
                    and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                    and l_quantity >= 1 and l_quantity <= 1 + 10
                    and p_size between 1 and 5) or 
                (
                    p_brand = 'Brand#23'
                    and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                    and l_quantity >= 10 and l_quantity <= 10 + 10
                    and p_size between 1 and 10) or 
                (
                    p_brand = 'Brand#34'
                    and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                    and l_quantity >= 20 and l_quantity <= 20 + 10
                    and p_size between 1 and 15
                )
    """)
    d = d.with_columns({"revenue": d["l_extendedprice"] * (1 - d["l_discount"])})
    result = d.aggregate(aggregations={"revenue":"sum"})
    result.explain()
    return result.collect()

def do_20():
    u_0 = lineitem.filter_sql("l_shipdate < date '1995-01-01' and l_shipdate >= date '1994-01-01'").groupby(["l_partkey", "l_suppkey"]).agg_sql("0.5 * SUM(l_quantity) AS sum_quantity").compute()
    u_3 = part.filter_sql("p_name like 'forest%'")
    u_4 = partsupp.join(qc.read_dataset(u_0), left_on="ps_suppkey", right_on="l_suppkey", how="inner")
    u_4 = u_4.join(u_3, left_on="ps_partkey", right_on="p_partkey", how="semi")
    u_4 = u_4.filter_sql("ps_availqty > sum_quantity and ps_partkey = l_partkey")
    d = supplier.join(u_4, left_on="s_suppkey", right_on="ps_suppkey", how="semi")
    d = d.join(nation.filter_sql("n_name = 'CANADA'"), left_on="s_nationkey", right_on="n_nationkey", how="inner")
    d = d.select(["s_name", "s_address"])
    d.explain()
    return d.collect().sort("s_name")

# the SQL for this is just probably not going to happen for a while
# this OOMs on cluster setup for SF100, probably ain't too great
def do_21():
    
    u_0 = lineitem.groupby("l_orderkey").agg_sql("ARRAY_AGG(l_suppkey) AS u_1").compute()
    u_1 = lineitem.filter_sql("l_receiptdate > l_commitdate").groupby("l_orderkey").agg_sql("ARRAY_AGG(l_suppkey) AS u_2").compute()

    d = lineitem.join(supplier, left_on="l_suppkey", right_on="s_suppkey", how="inner")
    d = d.join(orders, left_on="l_orderkey", right_on="o_orderkey", how="inner")
    d = d.filter_sql("l_receiptdate > l_commitdate and s_nationkey = 20 and o_orderstatus = 'F'")
    d = d.join(qc.read_dataset(u_0), left_on="l_orderkey", right_on="l_orderkey", how="inner", suffix = "_2")
    d = d.join(qc.read_dataset(u_1), left_on="l_orderkey", right_on="l_orderkey", how="left", suffix = "_3")
    d = d.with_column("cond1", lambda a: (a["u_2"].is_null()) | (~ (a["u_2"].arr.contains(a["l_suppkey"]) & (a["u_2"].arr.unique().arr.lengths() > 1))), required_columns={"l_suppkey", "u_2"})
    d = d.with_column("cond2", lambda a: a["u_1"].arr.contains(a["l_suppkey"]) & (a["u_1"].arr.unique().arr.lengths() > 1), required_columns={"l_suppkey", "u_1"})
    d = d.with_column("cond3", lambda x: x["cond1"] & x["cond2"], required_columns={"cond1", "cond2"})
    # d.explain()
    # return d.select(["u_1", "u_2", "l_suppkey", "cond1", "cond2", "cond3"]).top_k(["l_suppkey"], 100, descending=[False]).collect()
    d = d.filter_sql("cond3")
    d = d.groupby("s_name").agg_sql("count(*) AS numwait").top_k(["numwait", "s_name"], 100, descending=[True,False])
    d.explain()
    return d.collect()

def do_21_sql():

    # qc.set_config("optimize_joins", False)
    u_0 = lineitem.filter_sql("l_receiptdate > l_commitdate").select(["l_orderkey", "l_suppkey"])
    d = u_0.join(orders.filter_sql("o_orderstatus = 'F'").select(["o_orderkey"]), left_on="l_orderkey", right_on="o_orderkey", how="inner")
    d = d.join(supplier.filter_sql("s_nationkey = 20").select(["s_suppkey", "s_name"]), left_on="l_suppkey", right_on="s_suppkey", how="inner")

    u_1 = qc.read_parquet(s3_path_parquet + "lineitem.parquet/*").select(["l_orderkey", "l_suppkey"])
    # u_1 = qc.read_parquet(disk_path + "lineitem.parquet").select(["l_orderkey", "l_suppkey"])
    d = d.join(u_1, left_on="l_orderkey", right_on="l_orderkey", how="semi", suffix = "_2")
    print(u_1)

    u_2 = qc.read_parquet(s3_path_parquet + "lineitem.parquet/*").filter_sql("l_receiptdate > l_commitdate").select(["l_orderkey", "l_suppkey"])
    # u_2 = qc.read_parquet(disk_path + "lineitem.parquet").filter_sql("l_receiptdate > l_commitdate").select(["l_orderkey", "l_suppkey"])
   
    d = d.join(u_2, left_on="l_orderkey", right_on="l_orderkey", how="anti", suffix = "_3")

    d = d.groupby("s_name").agg_sql("count(*) AS numwait").top_k(["numwait", "s_name"], 100, descending=[True,False])

    d.explain()
    # qc.set_config("optimize_joins", False)
    return d.collect()
    
    

def do_22():
    u_0 = customer.filter_sql("""
        c_acctbal > 0.00
        AND SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
    """).agg_sql("AVG(c_acctbal) AS _col_0").collect()[0,0]

    d = customer.with_columns({"cntrycode": lambda x: x["c_phone"].str.slice(0, 2)}, required_columns={"c_phone"})
    d = d.filter_sql("cntrycode IN ('13', '31', '23', '29', '30', '18', '17') and c_acctbal > " + str(u_0))
    d = d.join(orders, left_on="c_custkey", right_on="o_custkey", how="anti")
    d = d.groupby("cntrycode").agg_sql("count(*) AS numcust, sum(c_acctbal) AS totacctbal")
    d.explain()
    return d.collect().sort('cntrycode')

def word_count():

    def udf2(x):
        x = x.to_arrow()
        da = compute.list_flatten(compute.ascii_split_whitespace(x["l_comment"]))
        c = da.value_counts().flatten()
        return polars.from_arrow(pa.Table.from_arrays([c[0], c[1]], names=["word","count"]))
    
    # words = qc.read_csv(disk_path + "random-words.txt",["text"],sep="|")
    counted = lineitem.transform( udf2, new_schema = ["word", "count"], required_columns = {"l_comment"}, foldable=True)
    f = counted.groupby("word").agg({"count":"sum"})
    return f.collect()

def dataset_test():
    z = orders.join(lineitem, left_on = "o_orderkey", right_on = "l_orderkey").filter_sql("o_orderkey > 100").select(["o_orderkey", "o_custkey"]).compute()
    print(z)
    s = qc.read_dataset(z)
    print(s)
    print(s.collect())

def print_and_time(f):
    start = time.time()
    print(f())
    end = time.time()
    print("query execution time: ", end - start)


def run_and_kill_after(f, ip, t ):
    import multiprocessing
    def killer():
        # Your code here
        count = 0
        while count < t:
            time.sleep(1)
            count += 1
        command = 'ssh -i /home/ubuntu/zihengw.pem ubuntu@' + ip + ' "/home/ubuntu/.local/bin/ray stop"'
        os.system(command)
    process = multiprocessing.Process(target = killer)
    process.start()
    start = time.time()
    print(f())
    end = time.time()
    print("query execution time: ", end - start)
    command = "/home/ubuntu/.local/bin/ray start --address=172.31.6.223:6380 --redis-password='5241590000000000'"
    launch_command = "ssh -oStrictHostKeyChecking=no -oConnectTimeout=5 -i /home/ubuntu/zihengw.pem ubuntu@" + ip + " '" + command.replace("'", "'\"'\"'") + "' "
    os.system(launch_command)
    process.terminate()
    process.join()

def covariance():

    return lineitem.gramian(["l_quantity", "l_extendedprice", "l_discount", "l_tax"]).collect()

def approx_quantile():

    return lineitem.approximate_quantile(["l_tax"], 0.9).collect()

# print_and_time(covariance)
# print_and_time(approx_quantile)

# from tpch_ref import *
queries = [None, do_1_sql, do_2, do_3_sql, do_4_sql, do_5_sql, do_6_sql, do_7_sql, do_8, do_9, 
           do_10, do_11, do_12_sql, do_13, do_14, do_15, do_16, do_17, do_18, do_19, do_20, None, do_22]
runtimes_16 = [
    None, 9.414733887, 8.524564743, 11.81241179, 15.07827894, 15.61281188, 3.801096042, 10.98091658, 15.76024667, 20.19576486, 12.24378769,
    4.671283484, 7.170949459, 10.30586807, 4.768202782, 5.819543203, 4.011099577, 23.80364641, 45.20878259, 13.02822073, 17.94240602, 51.86622826, 4.550808748]

print_and_time(queries[int(sys.argv[1])])
# print(do_1())
# print(do_1_1())
# print(do_1_2())
# print_and_time(do_1_sql)
# print_and_time(do_2)
# print_and_time(do_3_sql)
# print_and_time(do_4_sql)
# print_and_time(do_5_sql)
# print_and_time(do_6_sql)
# print_and_time(do_7_sql)
# print_and_time(do_8)
# print_and_time(do_9)
# run_and_kill_after(queries[int(sys.argv[1])], '18.236.218.228', runtimes_16[int(sys.argv[1])] // 2)
# print_and_time(do_10)
# print_and_time(do_11)
# print_and_time(do_12)
# print_and_time(do_12_sql)
# print_and_time(do_13) 
# print_and_time(do_14)
# print_and_time(do_15)
# print_and_time(do_16) 
# print_and_time(do_17)
# print_and_time(do_18)
# print_and_time(do_19)
# print_and_time(do_20)
# print_and_time(do_21_sql)
# print_and_time(do_22)


# print(do_21()) # just wn't work on AWS

# print(do_3_sql_blocking())
