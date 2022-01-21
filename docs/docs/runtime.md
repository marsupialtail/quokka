# Quokka Runtime API documentation

## Subtitle

*italic* 

[hyperlink](http://mittit.io/) 

    code

 **bold**. 

## Programming Model

The Quokka lower-level runtime API (henceforth referred to as the runtime API) allows you to construct a **task graph** of **nodes**, which each perform a specific task. This is very similar to other DAG-based processing frameworks such as [Apache Spark](https://spark.apache.org/docs/latest/) or [Tensorflow](https://www.tensorflow.org/). For example, you can write the following code in the runtime API to execute TPC-H query 6:
~~~python
    task_graph = TaskGraph()
    lineitem = task_graph.new_input_csv(bucket,key,lineitem_scheme,8,batch_func=lineitem_filter, sep="|")
    agg_executor = AggExecutor()
    agged = task_graph.new_stateless_node({0:lineitem}, agg_executor, 1, {0:None})
    task_graph.initialize()
    task_graph.run()
~~~

There are perhaps a couple of things to note here. Firstly, there are two types of nodes in the runtime API. There are **input nodes**, declared with APIs such as `new_input_csv` or `new_input_parquet`, which interface with the external world (you can define where they will read their data), and **task nodes**, declared with APIS such as `new_stateless_node` or `new_stateful_node`, which take as input the outputs generated from another node in the task graph, either an input node or another task node. In the example code above, we see that the task node `agged` depends on the outputs from the input node `lineitem`.

Quokka's task graph follows push-based execution. This means that a node does not wait for its downstream dependencies to ask for data, but instead actively *pushes* data to its downstream dependencies whenever some intermediate results become available. **In short, execution proceeds as follows**: input nodes read batches of data from a specified source, and pushes those batches to downstream task nodes. A task node exposes a handler to process incoming batches as they arrive, possibly updating some internal state (as an actor in an actor model), and for each input batch possibly produces an output batch for its own downstream dependencies. 

An input node completes execution when there's no more inputs to be read or if all of its downstream dependencies have completed execution. A task node completes execution when 

- all of its upstream sources have completed execution
- if its execution handler decides to terminate early based on the input batch and its state (e.g. for a task node that executes the limit operator in a limit query, it might keep as local state the buffered output, and decide to terminate when that output size surpasses the limit number)
- if all its downstream dependencies have completed execution.

By default, all task nodes start execution at once. This does not necessarily mean that they will start processing data, this means that they will all start waiting for input batches from their upstream sources to arrive. One could specify that an input node delay execution until another input node has finished. For example to implement a hash join one might want to stream in one table to build the hash table, then stream in the other table for probing. However, one cannot specify that a task node delay execution.