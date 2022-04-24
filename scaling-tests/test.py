from pyquokka.nodes import InputReaderNode, NonBlockingTaskNode
from pyquokka.sql import PolarJoinExecutor
from pyquokka.dataset import InputCSVDataset
import ray
import redis

ray.init(ignore_reinit_error=True)

NUM_MAPPERS = 2
NUM_JOINS = 4

r = redis.Redis(host="localhost",port=6800,db=0)
r.flushall()

lineitem_scheme = ["l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice", 
"l_discount","l_tax","l_returnflag","l_linestatus","l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct",
"l_shipmode","l_comment", "null"]
order_scheme = ["o_orderkey", "o_custkey","o_orderstatus","o_totalprice","o_orderdate","o_orderpriority","o_clerk",
"o_shippriority","o_comment", "null"]

lineitem_csv_reader = InputCSVDataset("tpc-h-csv", "lineitem/lineitem.tbl.1", lineitem_scheme , sep="|", stride = 128 * 1024 * 1024)
orders_csv_reader = InputCSVDataset("tpc-h-csv", "orders/orders.tbl.1", order_scheme , sep="|", stride = 128 * 1024 * 1024)
lineitem_csv_reader.get_csv_attributes(NUM_MAPPERS)
orders_csv_reader.get_csv_attributes(NUM_MAPPERS)

input_actors_a = {k: InputReaderNode.options(max_concurrency = 2, num_cpus = 0.001).remote(0,k,lineitem_csv_reader, NUM_MAPPERS, ("quokka-checkpoint","ckpt-0-" + str(k))) for k in range(NUM_MAPPERS)}
input_actors_b = {k: InputReaderNode.options(max_concurrency = 2, num_cpus = 0.001).remote(1,k,orders_csv_reader, NUM_MAPPERS, ("quokka-checkpoint","ckpt-1-" + str(k))) for k in range(NUM_MAPPERS)}
parents = {0:input_actors_a, 1: input_actors_b}
join_executor = PolarJoinExecutor(left_on="l_orderkey",right_on="o_orderkey")
join_actors = {i: NonBlockingTaskNode.options(max_concurrency = 2, num_cpus = 0.001).remote(2,i, {0:0,1:1}, None, join_executor, parents, ("quokka-checkpoint","ckpt-2-" + str(i))) for i in range(NUM_JOINS)}
join_channel_to_ip = {i: 'localhost' for i in range(NUM_JOINS)}

for j in range(NUM_MAPPERS):
    ray.get(input_actors_a[j].append_to_targets.remote((2, join_channel_to_ip, "l_orderkey")))
    ray.get(input_actors_b[j].append_to_targets.remote((2, join_channel_to_ip, "o_orderkey")))

handlers = {}
for i in range(NUM_MAPPERS):
    handlers[(0,i)] = input_actors_a[i].execute.remote()
    handlers[(1,i)] = input_actors_b[i].execute.remote()
for i in range(NUM_JOINS):
    handlers[(2,i)] = join_actors[i].execute.remote()

ray.get(list(handlers.values()))