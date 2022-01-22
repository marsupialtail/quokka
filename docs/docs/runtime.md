# Quokka Runtime API documentation

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

There are perhaps a couple of things to note here. Firstly, there are two types of nodes in the runtime API. There are **input nodes**, declared with APIs such as `new_input_csv` or `new_input_parquet`, which interface with the external world (you can define where they will read their data), and **task nodes**, declared with APIS such as `new_stateless_node` or `new_stateful_node` (not yet supported), which take as input the outputs generated from another node in the task graph, either an input node or another task node. In the example code above, we see that the task node `agged` depends on the outputs from the input node `lineitem`. Note that there are no special "output nodes", they are implemented as task nodes. 

Quokka's task graph follows push-based execution. This means that a node does not wait for its downstream dependencies to ask for data, but instead actively *pushes* data to its downstream dependencies whenever some intermediate results become available. **In short, execution proceeds as follows**: input nodes read batches of data from a specified source, and pushes those batches to downstream task nodes. A task node exposes a handler to process incoming batches as they arrive, possibly updating some internal state (as an actor in an actor model), and for each input batch possibly produces an output batch for its own downstream dependencies. The programmer is expected to supply this handler function as an **executor object** (more details later). Quokka provides a library of pre-implemented executor objects that the programmer can use for SQL, ML and graph analytics.

An input node completes execution when there's no more inputs to be read or if all of its downstream dependencies have completed execution. A task node completes execution when:

- all of its upstream sources have completed execution
- if its execution handler decides to terminate early based on the input batch and its state (e.g. for a task node that executes the limit operator in a limit query, it might keep as local state the buffered output, and decide to terminate when that output size surpasses the limit number)
- if all its downstream dependencies have completed execution.

By default, all task nodes start execution at once. This does not necessarily mean that they will start processing data, this means that they will all start waiting for input batches from their upstream sources to arrive. One could specify that an input node delay execution until another input node has finished. For example to implement a hash join one might want to stream in one table to build the hash table, then stream in the other table for probing. However, one cannot specify that a task node delay execution.

Each task node can have multiple physical executors, sometimes referred to as **channels**. This is a form of intra-operator parallelism, as opposed to the inter-operator parallelism that results from all task nodes executing at the same time. These physical executors all execute the same handler function, but on different portions of the input batch, partitioned by a user-specified partition key. A Spark-like map reduce with M mappers and R reducers would be implemented in Quokka as a single mapper task node and a single reducer task node, where the mapper task node has M channels and the reducer task node has R channels. In the example above, we specified that the input node `lineitem` has 8 channels, and the task node `agged` has only 1 channel. The partition key was not specified (`{0:None}`) since there is no parallelism, thus no need for partitioning. 

The runtime API is meant to be very flexible and support all manners of batch and stream processing. For example, one could specify an input node that listens to a Kafka stream, some task nodes which processes batches of data from that stream, and an output node that writes to another Kafka stream. In this case, since the input node will never terminate, and assuming the other nodes do not trigger early termination, the task graph will always be running.

As a result of this flexibility, it requires quite a lot of knowledge for efficient utilization. As a result, we aim to provide higher level APIs to support common batch and streaming tasks in SQL, machine learning and graph analytics. **Most programmers are not expected to program at the runtime API level, but rather make use of the pre-packaged higher-level APIs.**

## TaskGraph API

#### new_input_csv (bucket, key, names, parallelism, ip='localhost',batch_func=None, sep = ",", dependents = [], stride = 64 * 1024 * 1024)
Currently, new_input_csv only supports reading a CSV in batches from an AWS S3 bucket.

**Required arguments in order:**

- **bucket**: str. AWS S3 bucket
- **key**: str. AWS S3 key
- **names**: list of str. Column names. Note that if your rows ends with a delimiter value, such as in TPC-H, you will have to end this list with a placeholder such as "null". Look at the TPC-H code examples under apps.
- **parallelism**: int. the runtime API expects the programmer to explicitly state the amount of intra-op parallelism to expose. 8 is typically a good number.

**Keyword arguments:**

- **ip**: str. the IP address of the physical machine the input node should be placed. Defaults to local execution.
- **batch_func**: function. the user can optionally pass in a function to execute on the input CSV chunk before it's passed off to downstream dependents. Currently the input CSV is parsed into a Pandas Dataframe, so batch_func can be *any* Python function that can take a Pandas Dataframe as input and produces a Pandas Dataframe. This can be done to perform predicate pushdown for SQL for example.
- **sep**: str. delimiter
- **dependents**: list of int. an input node can depend on other input nodes, i.e. only start once another input node is done. For example to implement as hash join where one input might depend on another, one could do the following: 
```python
    a = new_input_csv(...)
    b = new_input_csv(...,dependents=[a])
```
- **stide**: int. how many bytes to read from the input S3 file to read at a time, default to 64 MB.

**Returns**: a node id which is a handle to this input node, that can be used as the sources argument for task nodes or dependents arguments for other input nodes.

#### new_input_parquet(bucket, key, names, parallelism, columns, skip_conditions, ip='localhost',batch_func=None, sep = ",", dependents = [], stride = 64 * 1024 * 1024)

Not yet implemented.

#### new_stateless_node(sources, functionObject, parallelism, partition_key, ip='localhost')
Instantiate a new task node with an executor object that defines the handler function which runs on each incoming batch.

**Required arguments in order:**

- **sources**: dict of int -> int. the upstream sources that feed batches to this task node. Expects a dictionary, where the keys are integers and values are node ids (also stored as integers). This in effect names the source nodes. i.e. if you specify `{0: source_node_id_x, 1:source_node_id_y}`, from the perspective of this task node you are calling the batches coming from source_node_id_x source 0 and the batches coming from node_id_y source 1. You will make use of these identifiers writing the executor class's handler function for incoming batches. 
- **functionObject**: an executor object which defines the input batch handler function. More details on this in the next section. You can write your own or use a pre-supplied one from the sql, ml or graph packages.
- **parallelism**: int. the runtime API expects the programmer to explicitly state the amount of intra-op parallelism to expose. Think carefully about this choice. Computationally intensive tasks might benefit from parallelism, while simple tasks such as aggregation might not.
- **partition_key**: dict of int -> in. This argument expects a dictionary with a key for each key in the sources dict. It describes how the input batches should be partitioned amongst the channels. If the value is None, then the input batch is copied and broadcast to all channels. Otherwise, currently each channel receives the sub-batch input_batch\[input_batch.partition_key % parallelism == channel_id\]. If this partition key is not in the input batch's columns from the specified source node, a runtime error would ensue. 

**Keyword arguments:**

- **ip**: str. the IP address of the physical machine the input node should be placed. Defaults to local execution.


## Writing Your Own (Stateless) Executor Object

The best place to learn how to write your own executor object classes is by looking at the available executor object classes in the SQL library. In short, an executor class is simply a child class of this base class: 

```python
class StatelessExecutor:

    def __init__(self) -> None:
        raise NotImplementedError

    def early_termination(self):
        self.early_termination = True

    def execute(self,batch,stream_id, executor_id):
        raise NotImplementedError

    def done(self,executor_id):
        raise NotImplementedError
```

The Stateless