
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
