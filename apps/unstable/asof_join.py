import time
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.sql import GroupAsOfJoinExecutor
from pyquokka.dataset import InputMultiCSVDataset

import redis
from schema import * 
r = redis.Redis(host="localhost", port=6800, db=0)
r.flushall()

ips = ['localhost', '172.31.11.134', '172.31.15.208', '172.31.11.188']
workers = 1

task_graph = TaskGraph()

quote_reader = InputMultiCSVDataset("quokka-asofjoin", "quotes", schema_quotes , stride = 128 * 1024 * 1024, header= True)
trades_reader = InputMultiCSVDataset("quokka-asofjoin", "trades", schema_trades , stride = 128 * 1024 * 1024, header= True)
quotes = task_graph.new_input_reader_node(quote_reader, {ip:16 for ip in ips[:workers]})
trades = task_graph.new_input_reader_node(trades_reader, {ip:8 for ip in ips[:workers]})

join_executor = GroupAsOfJoinExecutor(group_on="symbol", on="time")
output_stream = task_graph.new_non_blocking_node({0:trades,1:quotes},None,join_executor, {ip:4 for ip in ips[:workers]}, {0:"symbol", 1:"symbol"})

task_graph.create()
start = time.time()
task_graph.run_with_fault_tolerance()
print("total time ", time.time() - start)