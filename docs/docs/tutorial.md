#Advanced Tutorials

This section is for learning how to use Quokka's graph level API. This is expected for use cases where the dataframe API cannot satisfy your needs. Most users are not expected to program at this level. You should contact me: zihengw@stanford.edu if you want to do this.

You should probably stop reading now, unless you are a Stanford undergrad or masters student (or somebody else) who somehow decided to work with me on Quokka. 

The code for the tutorials can be found under `apps/tutorials`. They might perform meaningless tasks or perform tasks which you shoudn't necessarily use Quokka for, but they will showcase how Quokka works. 

I wrote Quokka. As a result I might take some things for granted that you might not. If you spot a typo or find some sections too difficult to understand, I would appreciate your feedback! Better yet, the docs are also open source under quokka/docs, so you can also make a PR.

## Lesson 0: Addition

Let's walk through our first Quokka program. This first example defines an input reader which produces a stream of numbers, and a stateful operator which adds them up. Please read the comments in the code. Let's first look at the import section. 

~~~python
# we need to import Quokka specific objects. A TaskGraph is always needed in a program 
# that uses the DAG runtime API. We will define a TaskGraph by defining input readers 
# and stateful operators and adding them to the TaskGraph. Then we will execute the TaskGraph.
from pyquokka.quokka_runtime import TaskGraph
# Quokka also needs a notion of the compute substrate the TaskGraph is executing on. 
# LocalCluster is meant for single-machine execution. For distributed execution, 
# you would need to import QuokkaClusterManager and create a new cluster or initialize 
# one from a json config. 
from pyquokka.utils import LocalCluster
# Executor is an abstract class which you should extend to implement your own executors. 
# Quokka also provides canned executors which you call import from pyquokka.executors such 
# as joins, sort and asof_join.
from pyquokka.executors import Executor
import time

# define a LocalCluster execution context. This will make a cluster object with information
# such as local core count etc.
cluster = LocalCluster()
~~~

Quokka provides many optimized input readers for different input data formats. However, in this tutorial we are going to define a custom input reader class to showcase how the input reader works. The mindset here is that there will be many channels of this input reader (by default equal to the number of cores in the cluster), and each channel will have its own copy of an object of this class. They will all be initialized in the same way, but when each channel calls the `get_next_batch` method of its own object, the `channel` argument supplied will be different. 

~~~python

class SimpleDataset:
    # the object will be initialized once locally. You can define whatever attributes you want.
    # You can also set attributes to None if they will be supplied later by the framework
    # in set_num_channels method
    def __init__(self, limit) -> None:
        self.limit = limit
        self.num_channels = None

    # this is an optional method that will be called by the runtime on this object during
    # TaskGraph construction, if the method exists. This mainly updates the num_channel
    # attribute of the object. For some input readers what a channel produces is independent
    # of the total number of channels, and they don't have to implement this method. Other
    # input readers might need to perform additional computation upon learning the total
    # number of channels, such as byte ranges to read in a CSV file. 
    # 
    # This method can be used to set additional class attributes. The programmer could 
    # do that in the __init__ method too, if she knows the total number of channels 
    # and does not want to rely on Quokka's default behavior etc.

    def set_num_channels(self, num_channels):
        self.num_channels = num_channels

    # the get_next_batch method defines an iterator. Each channel will iterate through
    # its own copy of the object's get_next_batch method, with the channel argument 
    # set to its own channel id. In this example, if there are N channels, channel k 
    # will yield numbers k, k + N, k + 2N, all the way up to the limit. 

    # Note that the get_next_batch method takes an optional parameter pos, and yields
    # two objects, with the first being None here. Let's not worry about these things
    # for the time being. They are used for Quokka's parallelized fault recovery.

    def get_next_batch(self, channel, pos=None):
        assert self.num_channels is not None
        curr_number =  channel
        while curr_number < self.limit:
            yield None, curr_number
            curr_number += self.num_channels

~~~

Now that we defined the input reader, we are going to define the stateful operator. Similar to the input reader, we define a Python class. All channels of the stateful operator will have a copy of an object of this class. The stateful operator exposes two important methods, `execute` and `done`, which might produce outputs for more downstream stateful operators. `execute` is called whenever upstream input reader channels have produced some input batches for the stateful operator channel to process. `done` is called when the stateful operator channel knows it will no longer receive any more inputs and has already processed all the inputs it has. Our stateful operator here adds up all the elements in an input stream and returns the sum. 

~~~python

class AddExecutor(Executor):
    # initialize state. This will be done locally. This initial state will be copied
    # along with the object to all the channels.
    def __init__(self) -> None:
        self.sum = 0
    
    # the execute method takes three arguments. The first argument batches, is a list of 
    # batches from an input QStream, which could be the output of an input reader or another
    # stateful operator. The items in the batch could have come from one channel, several, 
    # or all of them! it is best practice that the stateful operator doesn't make 
    # any assumptions on where these batches originated, except that they belong
    # to the same QStream. 

    # the second argument, stream_id, is used to identify the QStream the batches came from.
    # in this example we only have one input QStream so we can ignore this argument.

    # the third argument, channel, denotes the channel id of the channel executing the object
    # similar to the argument for the input reader. Here we also don't use this argument.

    def execute(self,batches,stream_id, channel):
        for batch in batches:
            assert type(batch) == int
            self.sum += batch
    # note that we can't return anything in our execute method. We don't know what the sum is
    # until we have seen all of the elements in the input QStream.
    
    # done only has one argument, which is the channel. It can return an element or an iterator
    # of elements.
    
    def done(self,channel):
        print("I am executor ", channel, " my sum is ", self.sum)
        return self.sum
~~~

Now that we have defined our input reader and stateful operator, we can hook them up together in a TaskGraph. Defining the TaskGraph requires a cluster object, which is LocalCluster here but can be an S3Cluster or AzureCluster for cloud deployments. 

We will then initialize the objects for the input reader and stateful operators. Again, we initialize one object, which will be copied to each channel. We can now add the input reader and stateful operator to our TaskGraph. 

~~~python

task_graph = TaskGraph(cluster)
reader = SimpleDataset(80)

# define a new input reader in our TaskGraph. numbers is a QStream.
numbers = task_graph.new_input_reader_node(reader)

executor = AddExecutor()

# define a new blocking node. A blocking node writes out its results in a materialized Dataset 
# object instead of producing a QStream. Note the first argument is a dictionary. This assigns
# each input stream an internal name, which corresponds to the stream_id field in the execute 
# method. Since we called the numbers QStream 0, when execute is called on batches from this QStream,
# the stream_id argument will be 0.
sum = task_graph.new_blocking_node({0:numbers},executor)

# create() must be called before run()
task_graph.create()

start = time.time()
task_graph.run()
print("total time ", time.time() - start)

# we can call to_list() on a Dataset object to collect its elements, which will simply be all 
# the objects returned by the blocking node's execute and done methods.
print(sum.to_list())

~~~

Here we used `new_blocking_node` to define the stateful operator in the TaskGraph. The TaskGraph exposes two different APIs: `new_nonblocking_node` and `new_blocking node`. The former will put their outputs in a QStream, which could be consumed by downstream operators immediately, while the latter will materialize the outputs into a Dataset object. Downstream operators cannot read a Dataset until it's complete. This is intimately related to the idea of nonblocking vs blocking stateful operators. Some operators such as streaming join can emit valid outputs as soon as they have seen partial inputs, while other operators like aggregation must wait until seeing all of the input before emitting any partial output. However, you could define a nonblocking operator as a `new_blocking_node`, if you want to materialize its outputs instead of streaming them forward, e.g. to limit pipeline depth. You could also define a blocking operator as a `new_nonblocking_node`, the QStream will just consist of the elements returned during the `done` method (which could return an iterator).

The TaskGraph also exposes an API to define stateless operators: `new_task`. This defines a stateless transform on a QStream and is very similar to Spark's `map`. We will cover this in a later tutorial to showcase deep learning inference. 

Note that we covered most of the important concepts covered in the getting started cartoons. However the astute reader would notice that we didn't define a partition function here, nor did we specify how many channels of the input reader or the stateful operator to launch. The answer is that Quokka tries to provide suitable defaults for these things. Quokka currently launches one channel per core for input readers, and one channel per machine for stateful operators. These defaults are subject to change and you shouldn't rely on them. Quokka's default partition function is to send all the outputs generated by a channel to the channel of the target on the same machine. 


## Lesson 1: Joins

If you think the first lesson was too complicated, it proably was. This is because we had to define custom input readers and stateful operators. Hopefully in the process you learned a few things about how Quokka works.

In most scenarios, it is my hope that you don't have to define custom objects, and use canned implementations which you can just import. This is similar to how Tensorflow or Pytorch works. If you know how to import `torch.nn.Conv2d`, you get the idea. 

Here, we are going to take two CSVs on Disk, join them, and count the number of records in the result: `select count(*) from a and b where a.key = b.key`. You can use the a.csv and b.csv provided in the apps/tutorials folder, or you can supply your own and change the CSV input reader arguments appropriately.

Without further ado, here's the code with comments:

~~~python
import time
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.executors import PolarJoinExecutor, CountExecutor
from pyquokka.dataset import InputDiskCSVDataset

from pyquokka.utils import LocalCluster

cluster = LocalCluster()

task_graph = TaskGraph(cluster)

# the arguments are: filename, column names, how many bytes to read in a batch
a_reader = InputDiskCSVDataset("a.csv", ["key","val1","val2"] , stride =  1024)
b_reader = InputDiskCSVDataset("b.csv",  ["key","val1","val2"] ,  stride =  1024)
a = task_graph.new_input_reader_node(a_reader)
b = task_graph.new_input_reader_node(b_reader)

# define a streaming join operator using the Polars library for internal join implementation. 
join_executor = PolarJoinExecutor(on="key")
# the default partition strategy will not work for join! We need to specify
# an alternative partition function. Quokka has the notion of "keyed" QStreams,
# which are QStreams where the batch elements are Pandas or Polars DataFrames 
# or Pyarrow tables. In this case, we can provide a column name as partition key.

joined = task_graph.new_non_blocking_node({0:a,1:b},join_executor,partition_key_supplied={0:"key", 1:"key"})
count_executor = CountExecutor()
count = task_graph.new_blocking_node({0:joined},count_executor)

task_graph.create()
start = time.time()
task_graph.run()
print("total time ", time.time() - start)

print(count.to_list())
~~~

Note here we defined a `new_nonblocking_node` for the join operator and a `new_blocking_node` for the count operator. This means that Quokka will execute the join in a pipelined parallel fashion with the count. As a result, the input reader, join and count actors are all executing concurrently in the system. The count operator will return the count as a single number which will be stored in a Dataset object.

About benchmarking Quokka programs. Quokka programs do a bit of processing locally. For example, when an input reader is added to the TaskGraph with an `InputDiskCSVDataset` object, Quokka performs `set_num_channels` on the object, and compute byte offsets for each channel to start reading from. This could be expensive for large CSV files, especially if we are using blob storage input sources. In pratice this completes in a few seconds for datasets TBs in size. This is quite similar to what Spark's dataframe API does.

The TaskGraph also needs to be initialized by calling `task_graph.create()`. This actually spawns the Ray actors executing the channels, and could take a while when you have a lot of channels. However, the time of both the input reader initialization and the TaskGraph initialization do not strongly scale with the input data size, unlike the actual execution time of the TaskGraph! As a result, while on trivial input sizes one might find the initialization times to be longer than the actual execution time, on real programs it is best practice to just time the `task_graph.run()` call. 

This example showed how to execute a simple SQL query by describing its physical plan. You can execute much mroe complex SQL queries with Quokka (check out the TPC-H implementations under quokka/apps). Quokka can currently typically achieve around 3x speedup compared to SparkSQL (EMR 6.5.0). If you have an expensive query you have to periodically run and would like to try writing out its physical plan in Quokka API, give it a shot! Again, contact me at zihengw@stanford.edu if you run into any problems.

We are working very hard to add a dataframe and SQL API to Quokka, targeting release Sep/Oct 2022. Keep tuned for more information.