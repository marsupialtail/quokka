create table lineitem (l_orderkey bigint, l_partkey bigint, l_suppkey bigint, l_linenumber bigint, l_quantity decimal(12,2), l_extendedprice decimal(12,2), l_discount decimal(12,2), l_tax decimal(12,2), l_returnflag char(1), l_linestatus char(1), l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct char(25), l_shipmode char(10), l_comment varchar ) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet/lineitem.parquet');
create table orders (o_orderkey bigint, o_custkey bigint, o_orderstatus char(1), o_totalprice decimal(12,2), o_orderdate date, o_orderpriority char(15), o_clerk char(15), o_shippriority int, o_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet/orders.parquet');
create table customer (c_custkey bigint, c_name char(25), c_address char(40), c_nationkey bigint, c_phone char(15), c_acctbal decimal(12,2), c_mktsegment char(10), c_comment char(17)) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet/customer.parquet');
create table part (p_partkey bigint, p_name varchar, p_mfgr char(25),p_brand char(10), p_type varchar ,p_size int, p_container char(10), p_retailprice decimal(12,2), p_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet/part.parquet');
create table supplier (s_suppkey bigint, s_name char(25), s_address varchar , s_nationkey bigint, s_phone char(15), s_acctbal decimal(12,2), s_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet/supplier.parquet');
create table partsupp (ps_partkey bigint, ps_suppkey bigint, ps_availqty bigint, ps_supplycost decimal(12,2), ps_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet/partsupp.parquet');
create table nation (n_nationkey bigint, n_name char(25), n_regionkey bigint , n_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet/nation.parquet');
create table region (r_regionkey bigint, r_name varchar, r_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet/region.parquet');

create table lineitem (l_orderkey bigint, l_partkey bigint, l_suppkey bigint, l_linenumber bigint, l_quantity decimal(12,2), l_extendedprice decimal(12,2), l_discount decimal(12,2), l_tax decimal(12,2), l_returnflag char(1), l_linestatus char(1), l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct char(25), l_shipmode char(10), l_comment varchar ) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet-1tb-iceberg/tpch1tb.db/lineitem/data/');
create table orders (o_orderkey bigint, o_custkey bigint, o_orderstatus char(1), o_totalprice decimal(12,2), o_orderdate date, o_orderpriority char(15), o_clerk char(15), o_shippriority int, o_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet-1tb-iceberg/tpch1tb.db/orders/data/');
create table customer (c_custkey bigint, c_name char(25), c_address char(40), c_nationkey bigint, c_phone char(15), c_acctbal decimal(12,2), c_mktsegment char(10), c_comment char(17)) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet-1tb-iceberg/tpch1tb.db/customer/data/');
create table part (p_partkey bigint, p_name varchar, p_mfgr char(25),p_brand char(10), p_type varchar ,p_size int, p_container char(10), p_retailprice decimal(12,2), p_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet-1tb-iceberg/tpch1tb.db/part/data/');
create table supplier (s_suppkey bigint, s_name char(25), s_address varchar , s_nationkey bigint, s_phone char(15), s_acctbal decimal(12,2), s_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet-1tb-iceberg/tpch1tb.db/supplier/data/');
create table partsupp (ps_partkey bigint, ps_suppkey bigint, ps_availqty bigint, ps_supplycost decimal(12,2), ps_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet-1tb-iceberg/tpch1tb.db/partsupp/data/');
create table nation (n_nationkey bigint, n_name char(25), n_regionkey bigint , n_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet-1tb-iceberg/tpch1tb.db/nation/data/');
create table region (r_regionkey bigint, r_name varchar, r_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet-1tb-iceberg/tpch1tb.db/region/data/');

create table lineitem (l_orderkey bigint, l_partkey bigint, l_suppkey bigint, l_linenumber bigint, l_quantity decimal(12,2), l_extendedprice decimal(12,2), l_discount decimal(12,2), l_tax decimal(12,2), l_returnflag char(1), l_linestatus char(1), l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct char(25), l_shipmode char(10), l_comment varchar ) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet-100-native-mine/lineitem.parquet/');
create table orders (o_orderkey bigint, o_custkey bigint, o_orderstatus char(1), o_totalprice decimal(12,2), o_orderdate date, o_orderpriority char(15), o_clerk char(15), o_shippriority int, o_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet-100-native-mine/orders.parquet/');
create table customer (c_custkey bigint, c_name char(25), c_address char(40), c_nationkey bigint, c_phone char(15), c_acctbal decimal(12,2), c_mktsegment char(10), c_comment char(17)) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet-100-native-mine/customer.parquet/');
create table part (p_partkey bigint, p_name varchar, p_mfgr char(25),p_brand char(10), p_type varchar ,p_size int, p_container char(10), p_retailprice decimal(12,2), p_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet-100-native-mine/part.parquet/');
create table supplier (s_suppkey bigint, s_name char(25), s_address varchar , s_nationkey bigint, s_phone char(15), s_acctbal decimal(12,2), s_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet-100-native-mine/supplier.parquet/');
create table partsupp (ps_partkey bigint, ps_suppkey bigint, ps_availqty bigint, ps_supplycost decimal(12,2), ps_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet-100-native-mine/partsupp.parquet/');
create table nation (n_nationkey bigint, n_name char(25), n_regionkey bigint , n_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet-100-native-mine/nation.parquet/');
create table region (r_regionkey bigint, r_name varchar, r_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-parquet-100-native-mine/region.parquet/');

create table lineitem (l_orderkey bigint, l_partkey bigint, l_suppkey bigint, l_linenumber bigint, l_quantity decimal(12,2), l_extendedprice decimal(12,2), l_discount decimal(12,2), l_tax decimal(12,2), l_returnflag char(1), l_linestatus char(1), l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct char(25), l_shipmode char(10), l_comment varchar ) WITH (format = 'parquet', external_location = 's3a://tpc-h-sf100-parquet/lineitem.parquet/');
create table orders (o_orderkey bigint, o_custkey bigint, o_orderstatus char(1), o_totalprice decimal(12,2), o_orderdate date, o_orderpriority char(15), o_clerk char(15), o_shippriority int, o_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-sf100-parquet/orders.parquet/');
create table customer (c_custkey bigint, c_name char(25), c_address char(40), c_nationkey bigint, c_phone char(15), c_acctbal decimal(12,2), c_mktsegment char(10), c_comment char(17)) WITH (format = 'parquet', external_location = 's3a://tpc-h-sf100-parquet/customer.parquet/');
create table part (p_partkey bigint, p_name varchar, p_mfgr char(25),p_brand char(10), p_type varchar ,p_size int, p_container char(10), p_retailprice decimal(12,2), p_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-sf100-parquet/part.parquet/');
create table supplier (s_suppkey bigint, s_name char(25), s_address varchar , s_nationkey bigint, s_phone char(15), s_acctbal decimal(12,2), s_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-sf100-parquet/supplier.parquet/');
create table partsupp (ps_partkey bigint, ps_suppkey bigint, ps_availqty bigint, ps_supplycost decimal(12,2), ps_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-sf100-parquet/partsupp.parquet/');
create table nation (n_nationkey bigint, n_name char(25), n_regionkey bigint , n_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-sf100-parquet/nation.parquet/');
create table region (r_regionkey bigint, r_name varchar, r_comment varchar) WITH (format = 'parquet', external_location = 's3a://tpc-h-sf100-parquet/region.parquet/');


analyze lineitem;
analyze orders;
analyze customer;
analyze part;
analyze supplier;
analyze partsupp;
analyze nation;
analyze region;


use default;
create table lineitem using parquet location 's3://tpc-h-sf100-parquet/lineitem.parquet/';
create table orders using parquet location 's3://tpc-h-sf100-parquet/orders.parquet/';
create table customer using parquet location 's3://tpc-h-sf100-parquet/customer.parquet/';
create table part using parquet location 's3://tpc-h-sf100-parquet/part.parquet/';
create table supplier using parquet location 's3://tpc-h-sf100-parquet/supplier.parquet/';
create table partsupp using parquet location 's3://tpc-h-sf100-parquet/partsupp.parquet/';
create table nation using parquet location 's3://tpc-h-sf100-parquet/nation.parquet/';
create table region using parquet location 's3://tpc-h-sf100-parquet/region.parquet/';

analyze table lineitem compute statistics for all columns;
analyze table orders compute statistics for all columns;
analyze table customer compute statistics for all columns;
analyze table part compute statistics for all columns;
analyze table supplier compute statistics for all columns;
analyze table partsupp compute statistics for all columns;
analyze table nation compute statistics for all columns;
analyze table region compute statistics for all columns;


create table lineitem (l_orderkey bigint, l_partkey bigint, l_suppkey bigint, l_linenumber bigint, l_quantity decimal(12,2), l_extendedprice decimal(12,2), l_discount decimal(12,2), l_tax decimal(12,2), l_returnflag varchar(1), l_linestatus varchar(1), l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct varchar(25), l_shipmode varchar(10), l_comment varchar(44) ) using csv options (header = "false", delimiter = "|", path = 's3://tpc-h-csv-100-mine/lineitem.tbl');
create table orders (o_orderkey bigint, o_custkey bigint, o_orderstatus varchar(1), o_totalprice decimal(12,2), o_orderdate date, o_orderpriority varchar(15), o_clerk varchar(15), o_shippriority int, o_comment varchar(79)) using csv options (header = "false", delimiter = "|", path = 's3://tpc-h-csv-100-mine/orders.tbl');
create table customer (c_custkey bigint, c_name varchar(25), c_address varchar(40), c_nationkey bigint, c_phone varchar(15), c_acctbal decimal(12,2), c_mktsegment varchar(10), c_comment varchar(117)) using csv options (header = "false", delimiter = "|", path = 's3://tpc-h-csv-100-mine/customer.tbl');
create table part (p_partkey bigint, p_name varchar(55), p_mfgr varchar(25),p_brand varchar(10), p_type varchar(25) ,p_size int, p_container varchar(10), p_retailprice decimal(12,2), p_comment varchar(23)) using csv options (header = "false", delimiter = "|", path = 's3://tpc-h-csv-100-mine/part.tbl');
create table supplier (s_suppkey bigint, s_name varchar(25), s_address varchar(25) , s_nationkey bigint, s_phone varchar(15), s_acctbal decimal(12,2), s_comment varchar(101)) using csv options (header = "false", delimiter = "|", path = 's3://tpc-h-csv-100-mine/supplier.tbl');
create table partsupp (ps_partkey bigint, ps_suppkey bigint, ps_availqty bigint, ps_supplycost decimal(12,2), ps_comment varchar(199)) using csv options (header = "false", delimiter = "|", path = 's3://tpc-h-csv-100-mine/partsupp.tbl');
create table nation (n_nationkey bigint, n_name varchar(25), n_regionkey bigint , n_comment varchar(152)) using csv options (header = "false", delimiter = "|", path = 's3://tpc-h-csv-100-mine/nation.tbl');
create table region (r_regionkey bigint, r_name varchar(25), r_comment varchar(152)) using csv options (header = "false", delimiter = "|", path = 's3://tpc-h-csv-100-mine/region.tbl');

schema_quotes = StructType()\
        .add("time", LongType(), True)\
        .add("symbol", StringType(), True)\
        .add("seq", FloatType(), True)\
        .add("bid", FloatType(), True)\
        .add("ask", FloatType(), True)\
        .add("bsize", FloatType(), True)\
        .add("asize", FloatType(), True)\
        .add("is_nbbo", BooleanType(), True)

schema_trades = StructType()\
        .add("time", LongType(), True)\
        .add("symbol", StringType(), True)\
        .add("size", FloatType(), True)\
        .add("price", FloatType(), True)

create table trade(time bigint, symbol VARCHAR, size REAL, price REAL) WITH (format = 'parquet', external_location = 's3://quokka-asof-parquet/trades/*');
create table quote using parquet location 's3://quokka-asof-parquet/quotes/*';

SELECT * FROM orders MATCH_RECOGNIZE(
     PARTITION BY custkey
     ORDER BY orderdate
     MEASURES
              A.totalprice AS starting_price,
              LAST(B.totalprice) AS bottom_price,
              LAST(U.totalprice) AS top_price
     ONE ROW PER MATCH
     AFTER MATCH SKIP PAST LAST ROW
     PATTERN (A B+ C+ D+)
     SUBSET U = (C, D)
     DEFINE
              B AS totalprice < PREV(totalprice),
              C AS totalprice > PREV(totalprice) AND totalprice <= A.totalprice,
              D AS totalprice > PREV(totalprice)
     )

CREATE TABLE test(
    a bigint,
    b bigint
) WITH (format = 'parquet', external_location = 'file:///home/hadoop/test.parquet');

CREATE TABLE crimes_c
(
    ID bigint,
    CASE_NUMBER varchar,
    BLOCK varchar,
    IUCR varchar,
    PRIMARY_CATEGORY varchar,
    DESCRIPTION varchar,
    LOCATION_DESC varchar,
    ARREST BOOLEAN,
    DOMESTIC BOOLEAN,
    BEAT bigint,
    DISTRICT double,
    WARD double,
    COMMUNITY double,
    FBI varchar,
    X double,
    Y double,
    YEAR bigint,
    UPDATED varchar,
    LATITUDE double,
    LONGITUDE double,
    TIMESTAMP_LONG bigint,
    PRIMARY_CATEGORY_ID integer
) WITH (format = 'parquet', external_location = 'file:///home/hadoop/crimes.parquet');

-- dataframe_schema is this {'ID:LONG': Int64, 'CASE:STRING': Utf8, 'BLOCK:STRING': Utf8, 'IUCR:STRING': Utf8, 'primary_category': Utf8, 'DESCRIPTION:STRING': Utf8, 'LOCATION_DESC:STRING': Utf8, 'ARREST:BOOLEAN': Boolean, 'DOMESTIC:BOOLEAN': Boolean, 'BEAT:SHORT': Int64, 'DISTRICT:FLOAT': Float64, 'WARD:FLOAT': Float64, 'COMMUNITY:FLOAT': Float64, 'FBI:STRING': Utf8, 'X:DOUBLE': Float64, 'Y:DOUBLE': Float64, 'YEAR:SHORT': Int64, 'UPDATED:STRING': Utf8, 'LATITUDE:DOUBLE': Float64, 'LONGITUDE:DOUBLE': Float64, 'TIMESTAMP:LONG': Int64, 'PRIMARY_CATEGORY_ID': Int32}
-- let's rename all the oclumns to match crimes_b in Python!!!

crimes_b.rename({'ID:LONG': 'ID', 'CASE:STRING': 'CASE_NUMBER', 'BLOCK:STRING': 'BLOCK', 'IUCR:STRING': 'IUCR', 'primary_category': 'PRIMARY_CATEGORY', 'DESCRIPTION:STRING': 'DESCRIPTION', 'LOCATION_DESC:STRING': 'LOCATION_DESC', 'ARREST:BOOLEAN': 'ARREST', 'DOMESTIC:BOOLEAN': 'DOMESTIC', 'BEAT:SHORT': 'BEAT', 'DISTRICT:FLOAT': 'DISTRICT', 'WARD:FLOAT': 'WARD', 'COMMUNITY:FLOAT': 'COMMUNITY', 'FBI:STRING': 'FBI', 'X:DOUBLE': 'X', 'Y:DOUBLE': 'Y', 'YEAR:SHORT': 'YEAR', 'UPDATED:STRING': 'UPDATED', 'LATITUDE:DOUBLE': 'LATITUDE', 'LONGITUDE:DOUBLE': 'LONGITUDE', 'TIMESTAMP:LONG': 'TIMESTAMP_LONG', 'PRIMARY_CATEGORY_ID': 'PRIMARY_CATEGORY_ID'}, inplace=True)

select * from crimes_c match_recognize(
    order by timestamp_long, id
    measures A.id as A_ID, B.id as B_ID, C.id as C_ID, D.id as D_ID, E.id as E_ID, F.id as F_ID, G.id as G_ID
    one row per match
    after match skip to next row
    pattern (A B C D E F G)
    define A as (primary_category_id = 19), B as ((primary_category_id = 4) AND ((latitude - A.latitude) >= -0.025) and ((latitude - A.latitude) <= 0.025) AND ((longitude - A.longitude) >= -0.025) and ((longitude - A.longitude) <= 0.025) AND (timestamp_long <= (A.timestamp_long + 1800000))), C as ((primary_category_id = 14) AND ((latitude - A.latitude) >= -0.025) and ((latitude - A.latitude) <= 0.025) AND ((longitude - A.longitude) >= -0.025) and ((longitude - A.longitude) <= 0.025) AND (timestamp_long <= (A.timestamp_long + 1800000))), D as ((primary_category_id = 29) AND ((latitude - A.latitude) >= -0.025) and ((latitude - A.latitude) <= 0.025) AND ((longitude - A.longitude) >= -0.025) and ((longitude - A.longitude) <= 0.025) AND (timestamp_long <= (A.timestamp_long + 1800000))), E as ((primary_category_id = 19) AND ((latitude - A.latitude) >= -0.025) and ((latitude - A.latitude) <= 0.025) AND ((longitude - A.longitude) >= -0.025) and ((longitude - A.longitude) <= 0.025) AND (timestamp_long <= (A.timestamp_long + 1800000))), F as ((primary_category_id = 32) AND ((latitude - A.latitude) >= -0.025) and ((latitude - A.latitude) <= 0.025) AND ((longitude - A.longitude) >= -0.025) and ((longitude - A.longitude) <= 0.025) AND (timestamp_long <= (A.timestamp_long + 1800000))), G as ((primary_category_id = 31) AND ((latitude - A.latitude) >= -0.025) and ((latitude - A.latitude) <= 0.025) AND ((longitude - A.longitude) >= -0.025) and ((longitude - A.longitude) <= 0.025) AND (timestamp_long <= (A.timestamp_long + 1800000)))
);

select * from crimes_c match_recognize(
    order by TIMESTAMP_LONG, ID
    measures A.ID as A_ID, B.ID as B_ID, C.ID as C_ID
    one row per match
    after match skip to next row
    pattern (A (G)* B (G)* C)
    define A as (PRIMARY_CATEGORY_ID = 27), G as (TIMESTAMP_LONG <= (A.TIMESTAMP_LONG + 20000000)), B as ((PRIMARY_CATEGORY_ID = 1) AND ((latitude - A.latitude) >= -0.025) and ((latitude - A.latitude) <= 0.025) AND ((longitude - A.longitude) >= -0.025) and ((longitude - A.longitude) <= 0.025)), C as ((PRIMARY_CATEGORY_ID = 24) AND ((latitude - A.latitude) >= -0.025) and ((latitude - A.latitude) <= 0.025) AND ((longitude - A.longitude) >= -0.025) and ((longitude - A.longitude) <= 0.025) AND (TIMESTAMP_LONG <= (A.TIMESTAMP_LONG + 20000000)))
);

WITH input_bucketized AS (
    SELECT *, cast(TIMESTAMP_LONG / (20000000) AS bigint) AS bk
    FROM crimes_c
), ranges AS (
    SELECT R.bk as bk_s, M.bk as bk_e
    FROM input_bucketized AS R, input_bucketized AS M
    WHERE R.bk = M.bk AND R.primary_category_id = 27
        AND M.primary_category_id = 24
        AND M.LONGITUDE BETWEEN R.LONGITUDE - 0.025 AND R.LONGITUDE + 0.025
        AND M.LATITUDE BETWEEN R.LATITUDE - 0.025 AND R.LATITUDE + 0.025
        AND M.TIMESTAMP_LONG - R.TIMESTAMP_LONG <= 20000000
    UNION
    SELECT R.bk as bk_s, M.bk as bk_e
    FROM input_bucketized AS R, input_bucketized AS M
    WHERE R.bk + 1 = M.bk AND R.primary_category_id = 27
        AND M.primary_category_id = 24
        AND M.LONGITUDE BETWEEN R.LONGITUDE - 0.025 AND R.LONGITUDE + 0.025
        AND M.LATITUDE BETWEEN R.LATITUDE - 0.025 AND R.LATITUDE + 0.025
        AND M.TIMESTAMP_LONG - R.TIMESTAMP_LONG <= 20000000
), buckets AS (
    SELECT DISTINCT bk
    FROM ranges
    CROSS JOIN UNNEST(sequence(bk_s, bk_e)) AS t(bk)
), prefilter AS (
    SELECT i.* FROM input_bucketized AS i, buckets AS b
    WHERE i.bk = b.bk
) SELECT * FROM prefilter MATCH_RECOGNIZE (
    order by TIMESTAMP_LONG, ID
    measures A.ID as A_ID, B.ID as B_ID, C.ID as C_ID
    one row per match
    after match skip to next row
    pattern (A (G)* B (G)* C)
    define A as (PRIMARY_CATEGORY_ID = 27), G as (TIMESTAMP_LONG <= (A.TIMESTAMP_LONG + 20000000)), 
    B as ((PRIMARY_CATEGORY_ID = 1) AND ((latitude - A.latitude) >= -0.025) and ((latitude - A.latitude) <= 0.025) AND ((longitude - A.longitude) >= -0.025) and ((longitude - A.longitude) <= 0.025)), 
    C as ((PRIMARY_CATEGORY_ID = 24) AND ((latitude - A.latitude) >= -0.025) and ((latitude - A.latitude) <= 0.025) AND ((longitude - A.longitude) >= -0.025) and ((longitude - A.longitude) <= 0.025) AND (TIMESTAMP_LONG <= (A.TIMESTAMP_LONG + 20000000)))
);

select * from crimes_b match_recognize(
    order by timestamp_long, id
    measures A.id as A_ID, B.id as B_ID, C.id as C_ID
    one row per match
    after match skip to next row
    pattern (A (Z)* B (Z)* C)
    define A as ((primary_category_id = 3) AND (beat = 2232)), Z as (timestamp_long <= (A.timestamp_long + 1800000)), B as ((primary_category_id = 31) AND (beat = 2232) AND (timestamp_long <= (A.timestamp_long + 1800000))), C as ((primary_category_id = 18) AND (beat = 2232) AND (timestamp_long <= (A.timestamp_long + 1800000)))
);

select count(*) from (select * from crimes_b match_recognize(
   order by timestamp_long, id
   measures A.id as A_ID, B.id as B_ID, A.timestamp_long as ats, B.timestamp_long as bts
    all rows per match
    after match skip to next row
    pattern (A (Z)* B)
    define A as ((primary_category_id = 27) AND (district = 14)), Z as (timestamp_long <= (A.timestamp_long + 50000000)), B as ((primary_category_id = 1) AND (district = 14) AND (timestamp_long <= (A.timestamp_long + 50000000)))
));

select count(*) from (select * from test1 match_recognize(
   order by ts
   measures A.a as a, B.b as b, A.ts as ats, B.ts as bts
    all rows per match
    after match skip to next row
    pattern (A (Z)* B)
    define A as (a = 1), Z as (ts <= (A.ts + 10)), B as ((b = 2) AND (ts <= (ts + 10)))
));



select
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        max(l_shipdate) as max_shipdate,
        max(l_commitdate) as max_commitdate,
        max(l_receiptdate) as max_receiptdate,
        sum(o_totalprice) as sum_charge,
        max(o_orderdate) as max_orderdate
from
        orders,
        lineitem
where
        o_orderkey = l_orderkey
        and l_shipmode in ('MAIL', 'SHIP', 'AIR', 'RAIL');


select
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        max(l_shipdate) as max_shipdate,
        max(l_commitdate) as max_commitdate,
        max(l_receiptdate) as max_receiptdate
from
        lineitem;