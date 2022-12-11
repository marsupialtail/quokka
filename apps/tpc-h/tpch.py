from pyquokka.df import * 
from pyquokka.utils import LocalCluster, QuokkaClusterManager
from schema import * 
mode = "S3"
format = "csv"
disk_path = "/home/ziheng/tpc-h/"
#disk_path = "s3://yugan/tpc-h-out/"
s3_path_csv = "s3://tpc-h-csv/"
s3_path_parquet = "s3://tpc-h-parquet/"

import pyarrow as pa
import pyarrow.compute as compute
import numpy as np
from pyquokka.executors import Executor
import polars

if mode == "DISK":
    cluster = LocalCluster()
elif mode == "S3":
    manager = QuokkaClusterManager()
    cluster = manager.get_cluster_from_json("config.json")
else:
    raise Exception

qc = QuokkaContext(cluster,4,2)

if mode == "DISK":
    if format == "csv":
        lineitem = qc.read_csv(disk_path + "lineitem.tbl", sep="|", has_header=True).drop(["null"])
        orders = qc.read_csv(disk_path + "orders.tbl", sep="|", has_header=True).drop(["null"])
        customer = qc.read_csv(disk_path + "customer.tbl",sep = "|", has_header=True).drop(["null"])
        part = qc.read_csv(disk_path + "part.tbl", sep = "|", has_header=True).drop(["null"])
        supplier = qc.read_csv(disk_path + "supplier.tbl", sep = "|", has_header=True).drop(["null"])
        partsupp = qc.read_csv(disk_path + "partsupp.tbl", sep = "|", has_header=True).drop(["null"])
        nation = qc.read_csv(disk_path + "nation.tbl", sep = "|", has_header=True).drop(["null"])
        region = qc.read_csv(disk_path + "region.tbl", sep = "|", has_header=True).drop(["null"])
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
        #lineitem = qc.read_parquet("s3://yugan/tpc-h-out/*")
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


# testa = qc.read_csv("test",list({
#     "key" : pyarrow.uint64(),
#     "val1": pyarrow.uint64(),
#     "val2": pyarrow.uint64(),
#     "val3": pyarrow.uint64(),
# }.keys()))

# testb = qc.read_csv("test",list({
#     "key" : pyarrow.uint64(),
#     "val1": pyarrow.uint64(),
#     "val2": pyarrow.uint64(),
#     "val3": pyarrow.uint64()
# }.keys()))


def do_1():

    '''
    Filters can be specified in SQL syntax. The columns in the SQL expression must exist in the schema of the DataStream.
    '''

    d = lineitem.filter("l_shipdate <= date '1998-12-01' - interval '90' day")
    d = d.with_column("disc_price", lambda x: x["l_extendedprice"] * (1 - x["l_discount"]), required_columns ={"l_extendedprice", "l_discount"})
    d = d.with_column("charge", lambda x: x["l_extendedprice"] * (1 - x["l_discount"]) * (1 + x["l_tax"]), required_columns={"l_extendedprice", "l_discount", "l_tax"})

    f = d.groupby(["l_returnflag", "l_linestatus"], orderby=["l_returnflag","l_linestatus"]).agg({"l_quantity":["sum","avg"], "l_extendedprice":["sum","avg"], "disc_price":"sum", 
        "charge":"sum", "l_discount":"avg","*":"count"})
        
    return f.collect()

def do_2():
    '''
    Quokka does not do query unnesting.
    '''
    europe = region.filter(region["r_name"] == "EUROPE")
    european_nations = nation.join(europe, left_on="n_regionkey",right_on="r_regionkey").select(["n_name","n_nationkey"])
    d = supplier.join(european_nations, left_on="s_nationkey", right_on="n_nationkey")
    d = partsupp.join(d, left_on="ps_suppkey", right_on="s_suppkey")
    f = d.groupby("ps_partkey").aggregate({"ps_supplycost":"min"}).collect()
    f = f.rename({"ps_supplycost_min":"min_cost","ps_partkey":"europe_key"})

    d = d.join(f, left_on="ps_supplycost", right_on="min_cost", suffix="_2")
    d = d.join(part, left_on="europe_key", right_on="p_partkey", suffix="_3")
    d = d.filter("""europe_key = ps_partkey and p_size = 15 and p_type like '%BRASS' """)
    d = d.select(["s_acctbal", "s_name", "n_name", "europe_key", "p_mfgr", "s_address", "s_phone", "s_comment"])

    d.explain()

    f = d.collect()
    f = f.sort(["s_acctbal","n_name","s_name","europe_key"],reverse=[True,False,False,False])[:100]

    
    return f


def do_3():
    d = lineitem.join(orders,left_on="l_orderkey", right_on="o_orderkey")
    d = customer.join(d,left_on="c_custkey", right_on="o_custkey")
    d = d.filter("c_mktsegment = 'BUILDING' and o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15'")
    d = d.with_column("revenue", lambda x: x["l_extendedprice"] * ( 1 - x["l_discount"]) , required_columns={"l_extendedprice", "l_discount"})

    #f = d.groupby(["l_orderkey","o_orderdate","o_shippriority"], orderby=[('revenue','desc'),('o_orderdate','asc')]).agg({"revenue":["sum"]})
    f = d.groupby(["l_orderkey","o_orderdate","o_shippriority"]).agg({"revenue":["sum"]})
    return f.collect()

def do_4():

    '''
    The DataFrame API does not (and does not plan to) do things like nested subquery decorrelation and common subexpression elimination.
    You should either do that yourself, or use the upcoming SQL API and rely on the decorrelation by SQLGlot.
    '''

    d = lineitem.filter("l_commitdate < l_receiptdate")
    #d = d.distinct("l_orderkey")
    d = orders.join(d, left_on="o_orderkey", right_on="l_orderkey", how = "semi")
    d = d.filter("o_orderdate >= date '1993-07-01' and o_orderdate < date '1993-10-01'")
    f = d.groupby("o_orderpriority").agg({'*':['count']})
    return f.collect()


def do_5():

    '''
    Quokka currently does not pick the best join order, or the best join strategy. This is upcoming improvement for a future release.
    You will have to pick the best join order. One way to do this is to do sparksql.explain and "borrow" Spark Catalyst CBO's plan.
    As a general rule of thumb you want to join small tables first and then bigger ones.
    '''

    asia = region.filter(region["r_name"] == "ASIA")
    asian_nations = nation.join(asia, left_on="n_regionkey",right_on="r_regionkey").select(["n_name","n_nationkey"])
    d = customer.join(asian_nations, left_on="c_nationkey", right_on="n_nationkey")
    d = d.join(orders, left_on="c_custkey", right_on="o_custkey", suffix="_3")
    d = d.join(lineitem, left_on="o_orderkey", right_on="l_orderkey", suffix="_4")
    d = d.join(supplier, left_on="l_suppkey", right_on="s_suppkey", suffix="_5")
    d = d.filter("s_nationkey = c_nationkey and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year")
    d = d.with_column("revenue", lambda x: x["l_extendedprice"] * ( 1 - x["l_discount"]) , required_columns={"l_extendedprice", "l_discount"})
    #f = d.groupby("n_name", orderby=[("revenue",'desc')]).agg({"revenue":["sum"]})
    f = d.groupby("n_name").agg({"revenue":["sum"]})

    return f.collect()

# def do_5():

#     '''
#     Quokka currently does not pick the best join order, or the best join strategy. This is upcoming improvement for a future release.
#     You will have to pick the best join order. One way to do this is to do sparksql.explain and "borrow" Spark Catalyst CBO's plan.
#     As a general rule of thumb you want to join small tables first and then bigger ones.
#     '''

    
#     d = customer.filter("c_nationkey IN (8, 9, 12, 18,21)").join(orders, left_on="c_custkey", right_on="o_custkey", suffix="_3")
#     d = d.join(lineitem, left_on="o_orderkey", right_on="l_orderkey", suffix="_4")
#     d = d.join(supplier, left_on="l_suppkey", right_on="s_suppkey", suffix="_5")
#     d = d.filter("s_nationkey = c_nationkey and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year")
#     d = d.with_column("revenue", lambda x: x["l_extendedprice"] * ( 1 - x["l_discount"]) , required_columns={"l_extendedprice", "l_discount"})
#     #f = d.groupby("n_name", orderby=[("revenue",'desc')]).agg({"revenue":["sum"]})
#     f = d.groupby("c_nationkey").agg({"revenue":["sum"]})

#     return f.collect()

def do_6():
    d = lineitem.filter("l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity < 24")
    d = d.with_column("revenue", lambda x: x["l_extendedprice"] * x["l_discount"], required_columns={"l_extendedprice", "l_discount"})
    f = d.aggregate({"revenue":["sum"]})
    return f.collect()

def do_7():
    d1 = customer.join(nation, left_on = "c_nationkey", right_on = "n_nationkey")
    d1 = d1.join(orders, left_on = "c_custkey", right_on = "o_custkey", suffix = "_3")
    d2 = supplier.join(nation, left_on="s_nationkey", right_on = "n_nationkey")
    d2 = lineitem.join(d2, left_on = "l_suppkey", right_on = "s_suppkey", suffix = "_3")
    
    d = d1.join(d2, left_on = "o_orderkey", right_on = "l_orderkey",suffix="_4")
    d = d.rename({"n_name_4": "supp_nation", "n_name": "cust_nation"})
    d = d.filter("""(
                                (supp_nation = 'FRANCE' and cust_nation = 'GERMANY')
                                or (supp_nation = 'GERMANY' and cust_nation = 'FRANCE')
                        )
                        and l_shipdate between date '1995-01-01' and date '1996-12-31'""")
    d = d.with_column("l_year", lambda x: x["l_shipdate"].dt.year(), required_columns = {"l_shipdate"})
    d = d.with_column("volume", lambda x: x["l_extendedprice"] * ( 1 - x["l_discount"]) , required_columns={"l_extendedprice", "l_discount"})
    f = d.groupby(["supp_nation","cust_nation","l_year"], orderby=["supp_nation","cust_nation","l_year"]).aggregate({"volume":"sum"})
    f.explain()
    return f.collect()

def do_8():
    america = region.filter(region["r_name"] == "AMERICA")
    american_nations = nation.join(america, left_on="n_regionkey",right_on="r_regionkey").select(["n_nationkey"])
    american_customers = customer.join(american_nations, left_on="c_nationkey", right_on="n_nationkey")
    american_orders = orders.join(american_customers, left_on = "o_custkey", right_on="c_custkey")
    d = lineitem.join(part, left_on="l_partkey", right_on="p_partkey")
    d = d.join(american_orders, left_on = "l_orderkey", right_on = "o_orderkey")
    d = d.join(supplier, left_on="l_suppkey", right_on="s_suppkey")
    d = d.join(nation, left_on="s_nationkey", right_on = "n_nationkey")
    d = d.filter("""
       o_orderdate between date '1995-01-01' and date '1996-12-31'
        and p_type = 'ECONOMY ANODIZED STEEL'         
    """)
    d = d.with_column("o_year", lambda x: x["o_orderdate"].dt.year(), required_columns = {"o_orderdate"})
    d = d.with_column("volume", lambda x: x["l_extendedprice"] * (1 - x["l_discount"]), required_columns = {"l_extendedprice", "l_discount"})
    d = d.rename({"n_name" : "nation"})
    d = d.with_column("brazil_volume", lambda x: x["volume"] * (x["nation"] == 'BRAZIL'), required_columns={"volume", "nation"})

    f = d.groupby("o_year").aggregate(aggregations={"volume":"sum", "brazil_volume":"sum"})
    f.explain()
    return f.collect()

# join ordering will be hard for this one
def do_9():
    d1 = supplier.join(nation, left_on="s_nationkey", right_on="n_nationkey")
    d2 = part.join(partsupp, left_on="p_partkey", right_on="ps_partkey")
    d2 = d2.filter("p_name like '%green%'")
    d = d2.join(d1, left_on="ps_suppkey", right_on = "s_suppkey")
    d = d.join(lineitem, left_on="p_partkey", right_on="l_partkey")
    d = d.join(orders, left_on = "l_orderkey", right_on = "o_orderkey")
    d = d.with_column("o_year", lambda x: x["o_orderdate"].dt.year(), required_columns = {"o_orderdate"})
    d = d.with_column("amount", lambda x: x["l_extendedprice"] * (1 - x["l_discount"]) - x["ps_supplycost"] * x["l_quantity"], required_columns = {"l_extendedprice", "l_discount", "ps_supplycost", "l_quantity"})
    d = d.rename({"n_name" : "nation"})
    
    f = d.groupby(["nation", "o_year"]).aggregate(aggregations = {"amount":"sum"})
    f.explain()
    result = f.collect()
    return result.sort('amount_sum')

def do_10():
    d = customer.join(nation, left_on="c_nationkey", right_on="n_nationkey")
    d = d.join(orders, left_on = "c_custkey", right_on = "o_orderkey")
    d = d.join(lineitem, left_on = "o_orderkey", right_on="l_orderkey")
    d = d.filter("""
        o_orderdate >= date '1993-10-01'
        and o_orderdate < date '1993-10-01' + interval '3' month
        and l_returnflag = 'R'
    """)
    f = d.groupby(["c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", \
        "c_address", "c_comment"]).aggregate()

def do_12():
    
    d = lineitem.join(orders,left_on="l_orderkey", right_on="o_orderkey")
    
    d = d.filter("l_shipmode IN ('MAIL','SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and \
        l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1995-01-01'")

    d = d.with_column("high", lambda x: (x["o_orderpriority"] == "1-URGENT") | (x["o_orderpriority"] == "2-HIGH"), required_columns={"o_orderpriority"})
    d = d.with_column("low", lambda x: (x["o_orderpriority"] != "1-URGENT") & (x["o_orderpriority"] != "2-HIGH"), required_columns={"o_orderpriority"})

    f = d.groupby("l_shipmode").aggregate(aggregations={'high':['sum'], 'low':['sum']})
    return f.collect()

def count():

    return lineitem.aggregate({"*":"count"}).collect()

def csv_to_parquet_disk():

    if not (mode == "DISK" and format == "csv"):
        return
    else:
        parquet = lineitem.write_parquet("/home/ziheng/tpc-h-out/", output_line_limit = 100000)
        return parquet

def csv_to_csv_disk():

    if not (mode == "DISK" and format == "csv"):
        return
    else:
        csvfiles = lineitem.write_csv("/home/ziheng/tpc-h-out/", output_line_limit = 1000000)
        return csvfiles

def csv_to_parquet_s3():

    if not (mode == "S3" and format == "csv"):
        return
    else:
        parquet = lineitem.write_parquet("s3://yugan/tpc-h-out/", output_line_limit = 5000000)
        return parquet
        

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

def covariance():

    class AggExecutor(Executor):
        def __init__(self) -> None:
            self.state = None
        def execute(self,batches,stream_id, executor_id):
            for batch in batches:
                #print(batch)
                if self.state is None:
                    self.state = batch
                else:
                    self.state += batch
        def done(self,executor_id):
            return self.state

    agg_executor = AggExecutor()
    def udf2(x):
        x = x.select(["l_quantity", "l_extendedprice", "l_discount", "l_tax"]).to_numpy()
        product = np.dot(x.transpose(), x)
        #print(product)
        return polars.from_numpy(product, columns = ["a","b","c","d"])

    d = lineitem.select(["l_quantity", "l_extendedprice", "l_discount", "l_tax"])
    d = d.transform( udf2, new_schema = ["a","b","c","d"], required_columns = {"l_quantity", "l_extendedprice", "l_discount", "l_tax"}, foldable=True)

    d = d.stateful_transform( agg_executor , ["a","b","c","d"], {"a","b","c","d"},
                           partitioner=BroadcastPartitioner(), placement_strategy = SingleChannelStrategy())
    
    return  d.collect()

def sort():

    return lineitem.drop("l_comment").sort("l_partkey", 200000000).write_parquet("s3://yugan/tpc-h-out/", output_line_limit = 5000000)

# print(count())
# print(csv_to_parquet_disk())
# print(csv_to_csv_disk())
# print(csv_to_parquet_s3())

# print(do_2())

# print(do_1())
# print(do_3())

# print(do_4())
print(do_5())
# print(do_6())
# print(do_7())
# print(do_8())
# print(do_9())
# print(do_12())

# print(word_count())
# print(covariance())
