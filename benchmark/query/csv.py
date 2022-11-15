from pyspark.sql.types import StructType, StructField, FloatType, LongType, DecimalType, IntegerType, StringType, DateType, BooleanType

"""
create table lineitem (l_orderkey bigint, l_partkey bigint, l_suppkey bigint, l_linenumber bigint, l_quantity decimal(12,2), l_extendedprice decimal(12,2), l_discount decimal(12,2), l_tax decimal(12,2), l_returnflag char(1), l_linestatus char(1), l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct char(25), l_shipmode char(10), l_comment varchar ) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet/lineitem.parquet');
create table orders (o_orderkey bigint, o_custkey bigint, o_orderstatus char(1), o_totalprice decimal(12,2), o_orderdate date, o_orderpriority char(15), o_clerk char(15), o_shippriority int, o_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet/orders.parquet');
create table customer (c_custkey bigint, c_name char(25), c_address char(40), c_nationkey bigint, c_phone char(15), c_acctbal decimal(12,2), c_mktsegment char(10), c_comment char(17)) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet/customer.parquet');
create table part (p_partkey bigint, p_name varchar, p_mfgr char(25),p_brand char(10), p_type varchar ,p_size int, p_container char(10), p_retailprice decimal(12,2), p_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet/part.parquet');
create table supplier (s_suppkey bigint, s_name char(25), s_address varchar , s_nationkey bigint, s_phone char(15), s_acctbal decimal(12,2), s_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet/supplier.parquet');
create table partsupp (ps_partkey bigint, ps_suppkey bigint, ps_availqty bigint, ps_supplycost decimal(12,2), ps_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet/partsupp.parquet');
create table nation (n_nationkey bigint, n_name char(25), n_regionkey bigint , n_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet/nation.parquet');
create table region (r_regionkey bigint, r_name varchar, r_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet/region.parquet');
"""

"""
create table lineitem_csv (l_orderkey varchar, l_partkey varchar, l_suppkey varchar, l_linenumber varchar, l_quantity varchar, l_extendedprice varchar, l_discount varchar, l_tax varchar, l_returnflag varchar, l_linestatus varchar, l_shipdate varchar, l_commitdate varchar, l_receiptdate varchar, l_shipinstruct  varchar, l_shipmode  varchar, l_comment varchar ) WITH (format = 'csv', csv_separator='|', external_location = 's3a://tpc-h-csv/lineitem-1/');
create table orders_csv (o_orderkey varchar, o_custkey varchar, o_orderstatus varchar, o_totalprice varchar, o_orderdate varchar, o_orderpriority varchar, o_clerk varchar, o_shippriority varchar, o_comment varchar) WITH (format = 'csv', csv_separator='|', external_location = 's3a://tpc-h-csv/orders-1/');
create table customer_csv (c_custkey varchar, c_name varchar, c_address varchar, c_nationkey varchar, c_phone varchar, c_acctbal varchar, c_mktsegment varchar, c_comment varchar) WITH (format = 'csv', csv_separator='|', external_location = 's3a://tpc-h-csv/customer-1/');
create table part_csv (p_partkey varchar, p_name varchar, p_mfgr varchar,p_brand varchar, p_type varchar ,p_size varchar, p_container varchar, p_retailprice varchar, p_comment varchar) WITH (format = 'csv', csv_separator='|', external_location = 's3a://tpc-h-csv/part-1/');
create table supplier_csv (s_suppkey varchar, s_name varchar, s_address varchar , s_nationkey varchar, s_phone varchar, s_acctbal varchar, s_comment varchar) WITH (format = 'csv', csv_separator='|', external_location = 's3a://tpc-h-csv/supplier-1/');
create table partsupp_csv (ps_partkey varchar, ps_suppkey varchar, ps_availqty varchar, ps_supplycost varchar, ps_comment varchar) WITH (format = 'csv', csv_separator='|', external_location = 's3a://tpc-h-csv/partsupp-1/');
create table nation_csv (n_nationkey varchar, n_name varchar, n_regionkey varchar , n_comment varchar) WITH (format = 'csv', csv_separator='|',external_location = 's3a://tpc-h-csv/nation/');
create table region_csv (r_regionkey varchar, r_name varchar, r_comment varchar) WITH (format = 'csv', csv_separator='|', external_location = 's3a://tpc-h-csv/region/');

create view lineitem as (
        select cast(l_orderkey as bigint) as l_orderkey,
               cast (l_partkey as bigint) as l_partkey,
               cast (l_suppkey as bigint) as l_suppkey,
               cast (l_linenumber as bigint) as l_linenumber,
               cast (l_quantity as decimal(12,2)) as l_quantity,
               cast (l_extendedprice as decimal(12,2)) as l_extendedprice,
               cast (l_discount as decimal(12,2)) as l_discount,
               cast (l_tax as decimal(12,2)) as l_tax,
               cast (l_returnflag as char(1)) as l_returnflag,
               cast (l_linestatus as char(1)) as l_linestatus,
               cast (l_shipdate as date) as l_shipdate,
               cast (l_commitdate as date) as l_commitdate,
               cast (l_receiptdate as date) as l_receiptdate,
               cast (l_shipinstruct as char(25)) as l_shipinstruct,
               cast (l_shipmode as char(10)) as l_shipmode,
               l_comment
        from lineitem_csv

); 
create view orders as (
        select cast (o_orderkey as bigint ) as o_orderkey,
	       cast (o_custkey as bigint ) as o_custkey,
	       cast (o_orderstatus as char(1) ) as o_orderstatus,
	       cast (o_totalprice as decimal(12,2) ) as o_totalprice,
	       cast (o_orderdate as date ) as o_orderdate,
	       cast (o_orderpriority as char(15) ) as o_orderpriority,
	       cast (o_clerk as char(15) ) as o_clerk,
	       cast (o_shippriority as int ) as o_shippriority,
	       o_comment
        from orders_csv
); 
create view  customer as (
        select cast (c_custkey as  bigint) as c_custkey,
	       cast (c_name as  char(25)) as c_name,
	       cast (c_address as char(40) ) as c_address,
	       cast (c_nationkey as  bigint) as c_nationkey,
	       cast (c_phone as  char(15)) as c_phone,
	       cast (c_acctbal as  decimal(12,2)) as c_acctbal,
	       cast (c_mktsegment as char(10) ) as c_mktsegment,
	       c_comment
        from customer_csv
);
create view  part as (
        select cast (p_partkey as bigint ) as p_partkey,
	       p_name,
	       cast (p_mfgr as char(25) ) as p_mfgr,
	       cast (p_brand as char(10) ) as p_brand,
	       p_type,
	       cast (p_size as int ) as p_size,
	       cast (p_container as char(10) ) as p_container,
	       cast (p_retailprice as decimal(12,2) ) as p_retailprice,
	       p_comment
        from part_csv
);
create view  partsupp as (
        select cast (ps_partkey as bigint ) as ps_partkey,
	            cast (ps_suppkey as bigint ) as ps_suppkey,
	            cast (ps_availqty as bigint ) as ps_availqty,
	            cast (ps_supplycost as decimal(12,2) ) as ps_supplycost,
	            ps_comment
        from partsupp_csv
);
create view supplier as (
        select      cast (s_suppkey as bigint ) as s_suppkey,
	            cast (s_name as char(25) ) as s_name,
	            s_address,
	            cast (s_nationkey as bigint ) as s_nationkey,
	            cast (s_phone as char(15) ) as s_phone,
	            cast (s_acctbal as decimal(12,2) ) as s_acctbal,
	            s_comment
        from supplier_csv
);
create view  nation as (
        select      cast (n_nationkey as  bigint) as n_nationkey,
	            cast (n_name as char(25) ) as n_name,
	            cast (n_regionkey as bigint ) as n_regionkey,
	            n_comment
        from nation_csv
);
create view  region as (
        select       cast (r_regionkey as bigint) as r_regionkey, 
                     r_name, 
                     r_comment
        from region_csv);

"""

"""
classification=trino-config,properties=[retry-policy=TASK]
classification=trino-connector-hive,properties=[hive.cache.enabled=false,hive.metastore-cache-ttl=0,hive.allow-drop-table=true]

"""

query1 = """
select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
from
        lineitem
where
        l_shipdate <= date '1998-12-01' - interval '90' day
group by
        l_returnflag,
        l_linestatus
order by
        l_returnflag,
        l_linestatus
"""

query2 = """
select
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment
from
        part,
        supplier,
        partsupp,
        nation,
        region
where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
        and p_size = 15
        and p_type like '%BRASS'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'EUROPE'
        and ps_supplycost = (
                select
                        min(ps_supplycost)
                from
                        partsupp,
                        supplier,
                        nation,
                        region
                where
                        p_partkey = ps_partkey
                        and s_suppkey = ps_suppkey
                        and s_nationkey = n_nationkey
                        and n_regionkey = r_regionkey
                        and r_name = 'EUROPE'
        )
order by
        s_acctbal desc,
        n_name,
        s_name,
        p_partkey
limit
        100
"""

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
limit
        10
"""

query4 = """
select
        o_orderpriority,
        count(*) as order_count
from
        orders
where
        o_orderdate >= date '1993-07-01'
        and o_orderdate < date '1993-07-01' + interval '3' month
        and exists (
                select
                        *
                from
                        lineitem
                where
                        l_orderkey = o_orderkey
                        and l_commitdate < l_receiptdate
        )
group by
        o_orderpriority
order by
        o_orderpriority
"""

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
        and r_name = 'ASIA'
        and o_orderdate >= date '1994-01-01'
        and o_orderdate < date '1994-01-01' + interval '1' year
group by
        n_name
order by
        revenue desc
"""

query6 = """
select
        sum(l_extendedprice * l_discount) as revenue
from
        lineitem
where
        l_shipdate >= date '1994-01-01'
        and l_shipdate < date '1994-01-01' + interval '1' year
        and l_discount between 0.06 - 0.01 and 0.06 + 0.01
        and l_quantity < 24

"""

query7 = """
select
        supp_nation,
        cust_nation,
        l_year,
        sum(volume) as revenue
from
        (
                select
                        n1.n_name as supp_nation,
                        n2.n_name as cust_nation,
                        extract(year from l_shipdate) as l_year,
                        l_extendedprice * (1 - l_discount) as volume
                from
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2
                where
                        s_suppkey = l_suppkey
                        and o_orderkey = l_orderkey
                        and c_custkey = o_custkey
                        and s_nationkey = n1.n_nationkey
                        and c_nationkey = n2.n_nationkey
                        and (
                                (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
                                or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
                        )
                        and l_shipdate between date '1995-01-01' and date '1996-12-31'
        ) as shipping
group by
        supp_nation,
        cust_nation,
        l_year
order by
        supp_nation,
        cust_nation,
        l_year
"""

query8 = """
select
        o_year,
        sum(case
                when nation = 'BRAZIL' then volume
                else 0
        end) / sum(volume) as mkt_share
from
        (
                select
                        extract(year from o_orderdate) as o_year,
                        l_extendedprice * (1 - l_discount) as volume,
                        n2.n_name as nation
                from
                        part,
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2,
                        region
                where
                        p_partkey = l_partkey
                        and s_suppkey = l_suppkey
                        and l_orderkey = o_orderkey
                        and o_custkey = c_custkey
                        and c_nationkey = n1.n_nationkey
                        and n1.n_regionkey = r_regionkey
                        and r_name = 'AMERICA'
                        and s_nationkey = n2.n_nationkey
                        and o_orderdate between date '1995-01-01' and date '1996-12-31'
                        and p_type = 'ECONOMY ANODIZED STEEL'
        ) as all_nations
group by
        o_year
order by
        o_year
"""

query9 = """
select
        nation,
        o_year,
        sum(amount) as sum_profit
from
        (
                select
                        n_name as nation,
                        extract(year from o_orderdate) as o_year,
                        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
                from
                        part,
                        supplier,
                        lineitem,
                        partsupp,
                        orders,
                        nation
                where
                        s_suppkey = l_suppkey
                        and ps_suppkey = l_suppkey
                        and ps_partkey = l_partkey
                        and p_partkey = l_partkey
                        and o_orderkey = l_orderkey
                        and s_nationkey = n_nationkey
                        and p_name like '%green%'
        ) as profit
group by
        nation,
        o_year
order by
        nation,
        o_year desc
"""

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
        and l_shipmode in ('MAIL', 'SHIP')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and l_receiptdate >= date '1994-01-01'
        and l_receiptdate < date '1994-01-01' + interval '1' year
group by
        l_shipmode
order by
        l_shipmode
"""
schema_lineitem = StructType()\
    .add("l_orderkey",LongType(),True)\
    .add("l_partkey",LongType(),True)\
    .add("l_suppkey",LongType(),True)\
    .add("l_linenumber",IntegerType(),True)\
    .add("l_quantity",DecimalType(10,2),True)\
    .add("l_extendedprice",DecimalType(10,2),True)\
    .add("l_discount",DecimalType(10,2),True)\
    .add("l_tax",DecimalType(10,2),True)\
    .add("l_returnflag",StringType(),True)\
    .add("l_linestatus",StringType(),True)\
    .add("l_shipdate",DateType(),True)\
    .add("l_commitdate",DateType(),True)\
    .add("l_receiptdate",DateType(),True)\
    .add("l_shipinstruct",StringType(),True)\
    .add("l_shipmode",StringType(),True)\
    .add("l_comment",StringType(),True)\
    .add("l_extra",StringType(),True)

schema_orders = StructType()\
    .add("o_orderkey",LongType(),True)\
    .add("o_custkey",LongType(),True)\
    .add("o_orderstatus",StringType(),True)\
    .add("o_totalprice",DecimalType(10,2),True)\
    .add("o_orderdate",DateType(),True)\
    .add("o_orderpriority",StringType(),True)\
    .add("o_clerk",StringType(),True)\
    .add("o_shippriority",IntegerType(),True)\
    .add("o_comment",StringType(),True)\
    .add("o_extra",StringType(),True)

schema_customers = StructType()\
    .add("c_custkey", LongType(), True)\
    .add("c_name", StringType(), True)\
    .add("c_address", StringType(), True)\
    .add("c_nationkey", LongType(), True)\
    .add("c_phone", StringType(), True)\
    .add("c_acctbal", DecimalType(10,2),True)\
    .add("c_mktsegment", StringType(), True)\
    .add("c_comment", StringType(), True)

schema_part = StructType()\
        .add("p_partkey", LongType(), True)\
        .add("p_name", StringType(), True)\
        .add("p_mfgr", StringType(), True)\
        .add("p_brand", StringType(), True)\
        .add("p_type", StringType(), True)\
        .add("p_size", IntegerType(), True)\
        .add("p_container", StringType(), True)\
        .add("p_retailprice", DecimalType(10,2), True)\
        .add("p_comment", StringType(), True)

schema_supplier = StructType([StructField("s_suppkey",LongType(),False),StructField("s_name",StringType(),True),StructField("s_address",StringType(),True),StructField("s_nationkey",LongType(),False),StructField("s_phone",StringType(),True),StructField("s_acctbal",DecimalType(10,2),True),StructField("s_comment",StringType(),True)])

schema_partsupp = StructType([StructField("ps_partkey",LongType(),False),StructField("ps_suppkey",LongType(),False),StructField("ps_availqty",IntegerType(),True),StructField("ps_supplycost",DecimalType(10,2),True),StructField("ps_comment",StringType(),True)])

schema_nation = StructType([StructField("n_nationkey",LongType(),False),StructField("n_name",StringType(),False),StructField("n_regionkey",LongType(),False),StructField("n_comment",StringType(),True)])

schema_region = StructType([StructField("r_regionkey",LongType(),False),StructField("r_name",StringType(),True),StructField("r_comment",StringType(),True)])



df_lineitem = spark.read.option("header", "false").option("delimiter","|")\
            .schema(schema_lineitem)\
            .csv("s3://tpc-h-csv/lineitem/lineitem.tbl.1")
df_orders = spark.read.option("header", "false").option("delimiter","|")\
        .schema(schema_orders)\
        .csv("s3://tpc-h-csv/orders/orders.tbl.1")
df_customers = spark.read.option("header", "false").option("delimiter","|")\
        .schema(schema_customers)\
        .csv("s3://tpc-h-csv/customer/customer.tbl.1")
df_partsupp = spark.read.option("header", "false").option("delimiter","|")\
            .schema(schema_partsupp)\
            .csv("s3://tpc-h-csv/partsupp/partsupp.tbl.1")
df_part = spark.read.option("header", "false").option("delimiter","|")\
        .schema(schema_part)\
        .csv("s3://tpc-h-csv/part/part.tbl.1")
df_supplier = spark.read.option("header", "false").option("delimiter","|")\
        .schema(schema_supplier)\
        .csv("s3://tpc-h-csv/supplier/supplier.tbl.1")
df_region = spark.read.option("header", "false").option("delimiter","|")\
            .schema(schema_region)\
            .csv("s3://tpc-h-csv/region/region.tbl")
df_nation = spark.read.option("header", "false").option("delimiter","|")\
        .schema(schema_nation)\
        .csv("s3://tpc-h-csv/nation/nation.tbl")


df_lineitem.createOrReplaceTempView("lineitem")
df_orders.createOrReplaceTempView("orders")
df_customers.createOrReplaceTempView("customer")
df_partsupp.createOrReplaceTempView("partsupp")
df_part.createOrReplaceTempView("part")
df_region.createOrReplaceTempView("region")
df_nation.createOrReplaceTempView("nation")
df_supplier.createOrReplaceTempView("supplier")

import time

start = time.time(); result = spark.sql(query12).collect(); print("QUERY TOOK", time.time() - start)

