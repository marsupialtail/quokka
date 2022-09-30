#Tutorials

This section is for learning how to use Quokka's dataframe API. Quokka's dataframe takes heavy inspiration from SparkSQL and Polars, and adopts a lazy execution model. This means that in contrast to Pandas, your operations are not executed immediately after you define them. Instead, Quokka builds a logical plan under the hood and executes it only when the user wants to "collect" the result, just like Spark. 

For the first part of our tutorial, we are going to go through implementing a few SQL queries in the TPC-H benchmark suite. You can download the data [here](https://drive.google.com/file/d/1a4yhPoknXgMhznJ9OQO3BwHz2RMeZr8e/view?usp=sharing). It is about 1GB unzipped.

## Lesson 0: Addition

Let's jump right in 

~~~python
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
~~~
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
    print(f)

    d = d.join(f, left_on="ps_supplycost", right_on="min_cost", suffix="_2")
    d = d.join(part, left_on="europe_key", right_on="p_partkey", suffix="_3")
    d = d.filter("""europe_key = ps_partkey and p_size = 15 and p_type like '%BRASS' """)
    d = d.select(["s_acctbal", "s_name", "n_name", "europe_key", "p_mfgr", "s_address", "s_phone", "s_comment"])
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
    d = d.distinct("l_orderkey")
    d = d.join(orders, left_on="l_orderkey", right_on="o_orderkey")
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
    return f.collect()

def do_12():
    
    d = lineitem.join(orders,left_on="l_orderkey", right_on="o_orderkey")
    
    d = d.filter("l_shipmode IN ('MAIL','SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and \
        l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1995-01-01'")

    d = d.with_column("high", lambda x: (x["o_orderpriority"] == "1-URGENT") | (x["o_orderpriority"] == "2-HIGH"), required_columns={"o_orderpriority"})
    d = d.with_column("low", lambda x: (x["o_orderpriority"] != "1-URGENT") & (x["o_orderpriority"] != "2-HIGH"), required_columns={"o_orderpriority"})

    f = d.groupby("l_shipmode").aggregate(aggregations={'high':['sum'], 'low':['sum']})
    return f.collect()