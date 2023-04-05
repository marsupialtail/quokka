from pyquokka.executors import *
from pyquokka.dataset import *
from pyquokka.logical import *
from pyquokka.target_info import *
from pyquokka.quokka_runtime import *
from pyquokka.expression import * 
from pyquokka.utils import EC2Cluster, LocalCluster
from pyquokka.sql_utils import required_columns_from_exp, label_sample_table_names
from functools import partial
import pyarrow as pa
from sqlglot.dataframe.sql import functions as F

class DataStream:

    """
    Quokka DataStream class is how most users are expected to interact with Quokka.
    However users are not expected to create a DataStream directly by calling its constructor.
    Note that constructor takes an argument called `source_node_id`, which would confuse 
    most data scientists -- even me!

    Args:
        quokka_context (pyquokka.df.QuokkaContext): Similar to Spark SQLContext.
        schema (list): The schema of this DataStream, i.e. a list of column names. We might change it to be 
            a dictionary with type information in the future to do better static code checking.
        source_node_id (int): the node in the logical plan that produces this DataStream.

    Attributes:
        quokka_context (pyquokka.df.QuokkaContext): Similar to Spark SQLContext.
        schema (list): The schema of this DataStream, i.e. a list of column names. We might change it to be 
            a dictionary with type information in the future to do better static code checking.
        source_node_id (int): the node in the logical plan that produces this DataStream.

    """

    def __init__(self, quokka_context, schema: list, source_node_id: int, sorted_reqs = None, materialized = False) -> None:
        self.quokka_context = quokka_context
        self.schema = schema
        self.source_node_id = source_node_id
        self.sorted = sorted_reqs
        self.materialized = materialized # this is used to indicate whether or not the source is materializable from a polars dataframe

    def _get_materialized_df(self):
        assert self.materialized == True
        return self.quokka_context.nodes[self.source_node_id].df
    
    def _set_materialized_df(self, df):
        assert type(df) == polars.DataFrame
        assert self.materialized == True
        self.quokka_context.nodes[self.source_node_id].df = df
        self.schema = df.columns

    def _set_sorted(self, sorted_reqs):
        """
        This is used to set the sorted attribute of this DataStream.
        Use with care! If the thing isn't actually sorted, shit will blow up.
        """
        assert type(sorted_reqs) == dict
        self.quokka_context.nodes[self.source_node_id].set_output_sorted_reqs(sorted_reqs)
        self.sorted = sorted_reqs

    def __str__(self):
        return "DataStream[" + ",".join(self.schema) + "]"

    def __repr__(self):
        return "DataStream[" + ",".join(self.schema) + "]"
    
    def __getitem__(self, key):
        assert key in self.schema, "Column " + key + " not in schema " + str(self.schema)
        return Expression(F.col(key))

    def collect(self):
        """
        This will trigger the execution of computational graph, similar to Spark collect(). 
        The result will be a Polars DataFrame returned to the client. 
        Like Spark, this will be slow or cause OOM if the result is very large!
        
        If you want to compute a temporary result that will be used in a future computation, try to use 
        the `compute()` method instead.

        Return:
            Polars DataFrame. 
        
        Examples:
            Result will be a Polars DataFrame, as if you did polars.read_csv("my_csv.csv")

            >>> f = qc.read_csv("my_csv.csv")
            >>> result = f.collect()  
        """
        if self.materialized:
            return self._get_materialized_df()

        dataset = self.quokka_context.new_dataset(self, self.schema)
        result = self.quokka_context.execute_node(dataset.source_node_id)
        return result

    def compute(self):
        """
        This will trigger the execution of computational graph, but store the result cached across the cluster.
        The result will be a Quokka DataSet. You can read a DataSet `x` back into a DataStream via `qc.read_dataset(x)`.
        This is similar to Spark's `persist()` method.

        Return:
            Quokka DataSet. This can be thought of as a list of objects cached in memory/disk across the cluster.
        
        Examples:

            >>> f = qc.read_csv("my_csv.csv")
            >>> result = f.collect()  
            >>> d = qc.read_dataset(result)
        """
        dataset = self.quokka_context.new_dataset(self, self.schema)
        return self.quokka_context.execute_node(dataset.source_node_id, collect=False)

    def explain(self, mode="graph"):
        '''
        This will not trigger the execution of your computation graph but will produce a graph of the execution plan. 
        Args:
            mode (str): 'graph' will show a graph, 'text' will print a textual description.
        Return:
            None.
        '''
        dataset = self.quokka_context.new_dataset(self, self.schema)
        return self.quokka_context.execute_node(dataset.source_node_id, explain=True, mode=mode)

    def write_csv(self, table_location, output_line_limit=1000000):
        """
        This will write out the entire contents of the DataStream to a list of CSVs. 

        Args:
            table_location (str): the root directory to write the output CSVs to. Similar to Spark, Quokka by default
                writes out a directory of CSVs instead of dumping all the results to a single CSV so the output can be
                done in parallel. If your dataset is small and you want a single file, you can adjust the output_line_limit
                parameter. Example table_locations: s3://bucket/prefix for cloud, absolute path /home/user/files for disk.
            output_line_limit (int): how many rows each CSV in the output should have. The current implementation simply buffers
                this many rows in memory instead of using file appends, so you should have enough memory!

        Return:
            DataStream containing the filenames of the CSVs that were produced. 
        
        Examples:

            >>> f = qc.read_csv("lineitem.csv")
            >>> f = f.filter("l_orderkey < 10 and l_partkey > 5")
            >>> f.write_csv("/home/user/test-out") 
            
            Make sure to create the directory first! This will write out a list of CSVs to /home/user/test-out.
        """

        assert "*" not in table_location, "* not supported, just supply the path."

        if self.materialized:
            df = self._get_materialized_df()
            df.write_csv(table_location)
            return

        if table_location[:5] == "s3://":

            if type(self.quokka_context.cluster) == LocalCluster:
                print(
                    "Warning: trying to write S3 dataset on local machine. This assumes high network bandwidth.")

            table_location = table_location[5:]
            bucket = table_location.split("/")[0]
            try:
                client = boto3.client("s3")
                region = client.get_bucket_location(Bucket=bucket)["LocationConstraint"]
            except:
                raise Exception("Bucket does not exist.")
            executor = OutputExecutor(
                table_location, "csv", region=region, row_group_size=output_line_limit)

        else:

            if type(self.quokka_context.cluster) == EC2Cluster:
                raise NotImplementedError(
                    "Does not support wQuokkariting local dataset with S3 cluster. Must use S3 bucket.")

            assert table_location[0] == "/", "You must supply absolute path to directory."
            assert os.path.isdir(
                table_location), "Must supply an existing directory"

            executor = OutputExecutor(
                table_location, "csv", region="local", row_group_size=output_line_limit)

        name_stream = self.quokka_context.new_stream(
            sources={0: self},
            partitioners={0: PassThroughPartitioner()},
            node=StatefulNode(
                schema=["filename"],
                # this is a stateful node, but predicates and projections can be pushed down.
                schema_mapping={"filename": {-1: "filename"}},
                required_columns={0: set(self.schema)},
                operator=executor
            ),
            schema=["filename"],
            
        )

        return name_stream

    def write_parquet(self, table_location, output_line_limit=5000000):

        """
        This will write out the entire contents of the DataStream to a list of Parquets. 

        Args:
            table_location (str): the root directory to write the output Parquets to. Similar to Spark, Quokka by default
                writes out a directory of Parquets instead of dumping all the results to a single Parquet so the output can be
                done in parallel. If your dataset is small and you want a single file, you can adjust the output_line_limit
                parameter. Example table_locations: s3://bucket/prefix for cloud, absolute path /home/user/files for disk.
            output_line_limit (int): the row group size in each output file.

        Return:
            DataStream containing the filenames of the Parquets that were produced. 
        
        Examples:

            >>> f = qc.read_csv("lineitem.csv")
            >>> f = f.filter("l_orderkey < 10 and l_partkey > 5")
            >>> f.write_parquet("/home/user/test-out") 
            
            You should create the directory before hand! This will write out a list of Parquets to /home/user/test-out.
            
        """

        if self.materialized:
            df = self._get_materialized_df()
            df.write_parquet(table_location)
            return

        if table_location[:5] == "s3://":

            if type(self.quokka_context.cluster) == LocalCluster:
                print(
                    "Warning: trying to write S3 dataset on local machine. This assumes high network bandwidth.")

            table_location = table_location[5:]
            bucket = table_location.split("/")[0]
            try:
                client = boto3.client("s3")
                region = client.get_bucket_location(Bucket=bucket)["LocationConstraint"]
            except:
                raise Exception("Bucket does not exist.")
            executor = OutputExecutor(
                table_location, "parquet", region=region, row_group_size=output_line_limit)

        else:

            if type(self.quokka_context.cluster) == EC2Cluster:
                raise NotImplementedError(
                    "Does not support writing local dataset with S3 cluster. Must use S3 bucket.")

            assert table_location[0] == "/", "You must supply absolute path to directory."

            executor = OutputExecutor(
                table_location, "parquet", region="local", row_group_size=output_line_limit)

        name_stream = self.quokka_context.new_stream(
            sources={0: self},
            partitioners={0: PassThroughPartitioner()},
            node=StatefulNode(
                schema=["filename"],
                # this is a stateful node, but predicates and projections can be pushed down.
                schema_mapping={"filename": {-1: "filename"}},
                required_columns={0: set(self.schema)},
                operator=executor
            ),
            schema=["filename"],
            
        )

        return name_stream
    
    def filter(self, predicate: Expression):

        """
        This will filter the DataStream to contain only rows that match a certain predicate specified in SQL syntax. 
        You can write any SQL clause you would generally put in a WHERE statement containing arbitrary conjunctions and 
        disjunctions. The columns in your statement must be in the schema of this DataStream! 

        Since a DataStream is implemented as a stream of batches, you might be tempted to think of a filtered DataStream
        as a stream of batches where each batch directly results from a filter being applied to a batch in the source DataStream. 
        While this certainly may be the case, filters are aggressively optimized by Quokka and is most likely pushed all the way down
        to the input readers. As a result, you typically should not see a filter node in a Quokka execution plan shown by `explain()`. 

        Args:
            predicate (Expression): an Expression.

        Return:
            A DataStream consisting of rows from the source DataStream that match the predicate.
        
        Examples:

            >>> f = qc.read_csv("lineitem.csv")

            Filter for all the rows where l_orderkey smaller than 10 and l_partkey greater than 5
            
            >>> f = f.filter((f["l_orderkey"] < 10) & (f["l_partkey"] > 5")) 
            
            Nested conditions are supported.
            
            >>> f = f.filter(f["l_orderkey"] < 10 & (f["l_partkey"] > 5 or f["l_partkey"] < 1)) 
            
            You can do some really complicated stuff! For details on the .str and .dt namespaces see the API reference.
            Quokka strives to support all the functionality of Polars, so if you see something you need that is not supported, please
            file an issue on Github.
            
            >>> f = f.filter((f["l_shipdate"].str.strptime().dt.offset_by(1, "M").dt.week() == 3) & (f["l_orderkey"] < 1000))
            
            This will fail! Assuming c_custkey is not in f.schema
            
            >>> f = f.filter(f["c_custkey"] > 10)
        """

        assert type(predicate) == Expression, "Must supply an Expression."
        return self.filter_sql(predicate.sql())

    def filter_sql(self, predicate: str):

        """
        This will filter the DataStream to contain only rows that match a certain predicate specified in SQL syntax. 
        You can write any SQL clause you would generally put in a WHERE statement containing arbitrary conjunctions and 
        disjunctions. The columns in your statement must be in the schema of this DataStream! 

        Since a DataStream is implemented as a stream of batches, you might be tempted to think of a filtered DataStream
        as a stream of batches where each batch directly results from a filter being applied to a batch in the source DataStream. 
        While this certainly may be the case, filters are aggressively optimized by Quokka and is most likely pushed all the way down
        to the input readers. As a result, you typically should not see a filter node in a Quokka execution plan shown by `explain()`. 

        Args:
            predicate (str): a SQL WHERE clause, look at the examples.

        Return:
            A DataStream consisting of rows from the source DataStream that match the predicate.
        
        Examples:

            Read in a CSV file into a DataStream f.

            >>> f = qc.read_csv("lineitem.csv")

            Filter for all the rows where l_orderkey smaller than 10 and l_partkey greater than 5.
            
            >>> f = f.filter_sql("l_orderkey < 10 and l_partkey > 5") 
            
            Nested conditions are supported.
            
            >>> f = f.filter_sql("l_orderkey < 10 and (l_partkey > 5 or l_partkey < 1)") 
            
            Most SQL features such as IN and date are supported. Anything DuckDB supports should work.
            
            >>> f = f.filter_sql("l_shipmode IN ('MAIL','SHIP') and l_receiptdate < date '1995-01-01'")
            
            You can do arithmetic in the predicate just like in SQL. 
            
            >>> f = f.filter_sql("l_shipdate < date '1994-01-01' + interval '1' year and l_discount between 0.06 - 0.01 and 0.06 + 0.01")
            
            This will fail! Assuming c_custkey is not in f.schema
            
            >>> f = f.filter_sql("c_custkey > 10")
        """

        assert type(predicate) == str
        predicate = sqlglot.parse_one(predicate)
        # convert to CNF
        predicate = optimizer.normalize.normalize(predicate, dnf = False)

        columns = set(i.name for i in predicate.find_all(
            sqlglot.expressions.Column))
        for column in columns:
            assert column in self.schema, "Tried to filter on a column not in the schema {}".format(column)
        
        if self.materialized:
            batch_arrow = self._get_materialized_df().to_arrow()
            con = duckdb.connect().execute('PRAGMA threads=%d' % 8)
            df = polars.from_arrow(con.execute("select * from batch_arrow where " + predicate.sql(dialect = "duckdb")).arrow())
            return self.quokka_context.from_polars(df)

        if not optimizer.normalize.normalized(predicate):
            def f(df):
                batch_arrow = df.to_arrow()
                con = duckdb.connect().execute('PRAGMA threads=%d' % 8)
                return polars.from_arrow(con.execute("select * from batch_arrow where " + predicate.sql(dialect = "duckdb")).arrow())
        
            transformed = self.transform(f, new_schema = self.schema, required_columns=self.schema)
            return transformed
        else:
            return self.quokka_context.new_stream(sources={0: self}, partitioners={0: PassThroughPartitioner()}, node=FilterNode(self.schema, predicate),
                                              schema=self.schema, sorted = self.sorted)

    def select(self, columns: list):

        """
        This will create a new DataStream that contains only selected columns from the source DataStream.

        Since a DataStream is implemented as a stream of batches, you might be tempted to think of a filtered DataStream as a stream of batches where each
        batch directly results from selecting columns from a batch in the source DataStream. While this certainly may be the case, `select()` is aggressively 
        optimized by Quokka and is most likely pushed all the way down to the input readers. As a result, you typically should
        not see a select node in a Quokka execution plan shown by `explain()`. 

        It is much better to think of a DataStream simply as a stream of rows that meet certain criteria, and who may be non-deterministically 
        batched together by the Quokka runtime. Indeed, Quokka makes no guarantees on the sizes of these batches, which is determined at runtime. 
        This flexibility is an important reason for Quokka's superior performance.

        Args:
            columns (list): a list of columns to select from the source DataStream

        Return:
            A DataStream consisting of only the columns selected.
        
        Examples:

            >>> f = qc.read_csv("lineitem.csv")

            Select only the l_orderdate and l_orderkey columns

            >>> f = f.select(["l_orderdate", "l_orderkey"])

            This will now fail, since f's schema now consists of only two columns.

            >>> f = f.select(["l_linenumber"])
        """

        assert type(columns) == set or type(columns) == list

        for column in columns:
            assert column in self.schema, "Projection column not in schema"
        
        if self.materialized:
            df = self._get_materialized_df().select(columns)
            return self.quokka_context.from_polars(df)

        return self.quokka_context.new_stream(
            sources={0: self},
            partitioners={0: PassThroughPartitioner()},
            node=ProjectionNode(set(columns)),
            schema=columns,
            sorted = self.sorted
            )

    def drop(self, cols_to_drop: list):

        """
        Think of this as the anti-opereator to select. Instead of selecting columns, this will drop columns. 
        This is implemented in Quokka as selecting the columns in the DataStream's schema that are not dropped.

        Args:
            cols_to_drop (list): a list of columns to drop from the source DataStream

        Return:
            A DataStream consisting of all columns in the source DataStream that are not in `cols_to_drop`.
        
        Examples:
            >>> f = qc.read_csv("lineitem.csv")

            Drop the l_orderdate and l_orderkey columns

            >>> f = f.drop(["l_orderdate", "l_orderkey"])

            This will now fail, since you dropped l_orderdate

            >>> f = f.select(["l_orderdate"])
        """
        assert type(cols_to_drop) == list
        actual_cols_to_drop = []
        for col in cols_to_drop:
            if col in self.schema:
                actual_cols_to_drop.append(col)
            if self.sorted is not None:
                assert col not in self.sorted, "cannot drop a sort key!"
        if len(actual_cols_to_drop) == 0:
            return self
        else:
            if self.materialized:
                df = self._get_materialized_df().drop(actual_cols_to_drop)
                return self.quokka_context.from_polars(df)
            else:
                return self.select([col for col in self.schema if col not in cols_to_drop])

    def rename(self, rename_dict):

        """
        Renames columns in the DataStream according to rename_dict. This is similar to 
        [`polars.rename`](https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.DataFrame.rename.html).
        The keys you supply in rename_dict must be present in the schema, and the rename operation
        must not lead to duplicate column names.

        Note this will lead to a physical operation at runtime. This might also complicate join reodering, so should be avoided if possible.

        Args:
            rename_dict (dict): key is old column name, value is new column name.

        Return:
            A DataStream with new schema according to rename. 
        
        Examples:
            >>> f = qc.read_csv("lineitem.csv")

            Rename the l_orderdate and l_orderkey columns

            >>> f = f.rename({"l_orderdate": "orderdate", "l_orderkey": "orderkey"})

            This will now fail, since you renamed l_orderdate

            >>> f = f.select(["l_orderdate"])
        """

        new_sorted = {}
        assert type(
            rename_dict) == dict, "must specify a dictionary like Polars"
        for key in rename_dict:
            assert key in self.schema, "key in rename dict must be in schema"
            assert rename_dict[key] not in self.schema, "new name must not be in current schema"
            if self.sorted is not None and key in self.sorted:
                new_sorted[rename_dict[key]] = self.sorted[key]
        
        if self.materialized:
            df = self._get_materialized_df().rename(rename_dict)
            return self.quokka_context.from_polars(df)

        # the fact you can write this in one line is why I love Python
        new_schema = [col if col not in rename_dict else rename_dict[col]
                      for col in self.schema]
        schema_mapping = {}
        for key in rename_dict:
            schema_mapping[rename_dict[key]] = {0: key}
        for key in self.schema:
            if key not in rename_dict:
                schema_mapping[key] = {0: key}

        def f(x): return x.rename(rename_dict)

        return self.quokka_context.new_stream(
            sources={0: self},
            partitioners={0: PassThroughPartitioner()},
            node=MapNode(
                schema=new_schema,
                schema_mapping=schema_mapping,
                required_columns={0: set(rename_dict.keys())},
                function=f,
                foldable=True
            ),
            schema=new_schema,
            sorted = new_sorted if len(new_sorted) > 0 else None
            
        )

    def transform(self, f, new_schema: list, required_columns: set, foldable=True):

        """
        This is a rather Quokka-specific API that allows arbitrary transformations on a DataStream, similar to Spark RDD.map.
        Each batch in the DataStream is going to be transformed according to a user defined function, which can produce a new batch.
        The new batch can have completely different schema or even length as the original batch, and the original data is considered lost,
        or consumed by this transformation function. This could be used to implement user-defined-aggregation-functions (UDAFs). Note in
        cases where you are simply generating a new column from other columns for each row, i.e. UDF, you probably want to use the 
        `with_columns` method instead. 

        A DataStream is implemented as a stream of batches. In the runtime, your transformation function will be applied to each of those batches.
        However, there are no guarantees whatsoever on the sizes of these batches! You should probably make sure your logic is correct
        regardless of the sizes of the batches. 
        
        For example, if your DataStream consists of a column of numbers, and you wish to compute the sum
        of those numbers, you could first transform the DataStream to return just the sum of each batch, and then hook this DataStream up to 
        a stateful operator that adds up all the sums. 

        You can use whatever libraries you have installed in your Python environment in this transformation function. If you are using this on a
        cloud cluster, you have to make sure the necessary libraries are installed on each machine. You can use the `utils` package in pyquokka to help
        you do this, in particular, check out `manager.install_python_package`.

        Note a transformation in the logical plan basically precludes any predicate pushdown or early projection past it, since the original columns 
        are assumed to be lost, and we cannot directly establish correspendences between the input columns to a transformation and its output 
        columns for the purposes of predicate pushdown or early projection. The user is required to supply a set or list of required columns,
        and we will select for those columns (which can be pushed down) before we apply the transformation. 

        Args:
            f (function): The transformation function. This transformation function must take as input a Polars DataFrame and output a Polars DataFrame. 
                The transformation function must not have expectations on the length of its input. Similarly, the transformation function does not 
                have to emit outputs of a specific size. The transformation function must produce the same output columns for every possible input.
            new_schema (list): The names of the columns of the Polars DataFrame that the transformation function produces. 
            required_columns (set): The names of the columns that are required for this transformation. This argument is made mandatory
                because it's often trivial to supply and can often greatly speed things up.
            foldable (bool): Whether or not the transformation can be executed as part of the batch post-processing of the previous operation in the 
                execution graph. This is set to True by default. Correctly setting this flag requires some insight into how Quokka works. Lightweight
                functions generally benefit from being folded. Heavyweight functions or those whose efficiency improve with large input sizes 
                might benefit from not being folded. 

        Return:
            A new transformed DataStream with the supplied schema.
        
        Examples:

            Let's define a user defined function that takes in a Polars DataFrame with a single column "text", converts it to a Pyarrow table,
            and uses nice Pyarrow compute functions to perform the word count on this Polars DataFrame. Note 1) we have to convert it 
            back to a Polars DataFrame afterwards, 2) the function works regardless of input length and 3) the output columns are the 
            same regardless of the input.

            >>> def udf2(x):
            >>>    x = x.to_arrow()
            >>>    da = compute.list_flatten(compute.ascii_split_whitespace(x["text"]))
            >>>    c = da.value_counts().flatten()
            >>>    return polars.from_arrow(pa.Table.from_arrays([c[0], c[1]], names=["word","count"]))

            This is a trick to read in text files, just use read_csv with a separator you know won't appear -- the result will just be DataStream with one column. 
            
            >>> words = qc.read_csv("random_words.txt", ["text"], sep = "|")

            Now transform words to counts. The result will be a DataStream with two columns, "word" and "count". 

            >>> counted = words.transform( udf2, new_schema = ["word", "count"], required_columns = {"text"}, foldable=True)

        """
        if type(required_columns) == list:
            required_columns = set(required_columns)
        assert type(required_columns) == set

        if self.materialized:
            df = self._get_materialized_df()
            df = f(df)
            return self.quokka_context.from_polars(df)

        select_stream = self.select(required_columns)

        return self.quokka_context.new_stream(
            sources={0: select_stream},
            partitioners={0: PassThroughPartitioner()},
            node=MapNode(
                schema=new_schema,
                schema_mapping={col: {-1: col} for col in new_schema},
                required_columns={0: required_columns},
                function=f,
                foldable=foldable
            ),
            schema=new_schema,
            
        )
        
    def transform_sql(self, sql_expression, groupby = [], foldable = True):

        """

        This is a SQL version of the `transform` method. It allows you to write SQL expressions that can be applied to each batch in the DataStream.
        This is the X in `select X from Y`. The Y is the DataStream. The X can be any SQL expression, including aggregation functions.
        Example sql expression:
            "
            sum(l_quantity) as sum_qty,
            sum(l_extendedprice) as sum_base_price,
            sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge"

        You must supply an alias for each transformation. Any syntax that is supported by DuckDB can be used. 
        You can optionally also specify groupby columns. This will apply the SQL expression `select X from Y group by Z` to each batch in the DataStream.

        Args:
            sql_expression (str): The SQL expression to apply to each batch in the DataStream.
            groupby (list): The list of columns to group by.
            foldable (bool): Whether or not the transformation can be executed as part of the batch post-processing of the previous operation in the
                execution graph. This is set to True by default. Probably should be True.
        
        """

        assert type(groupby) == list

        enhanced_exp = "select " +",".join(groupby) + ", " + sql_expression + " from batch_arrow"
        if len(groupby) > 0:
            enhanced_exp = enhanced_exp + " group by " + ",".join(groupby)

        enhanced_exp = label_sample_table_names(sqlglot.parse_one(enhanced_exp), 'batch_arrow').sql()

        sqlglot_node = sqlglot.parse_one(enhanced_exp)
        required_columns = required_columns_from_exp(sqlglot_node)
        if len(required_columns) == 0:
            # TODO: this is to make sure count works by picking a random column to download. 
            required_columns = {self.schema[1]}
        
        for col in required_columns:
            assert col in self.schema, "required column %s not in schema" % col
        
        new_columns = [i.alias for i in sqlglot_node.selects if i.name not in groupby]
        assert '' not in new_columns, "must provide alias for each computation"

        assert type(required_columns) == set
        for column in new_columns:
            assert column not in self.schema, "For now new columns cannot have same names as existing columns"
        
        if self.materialized:
            batch_arrow = self._get_materialized_df().to_arrow()
            con = duckdb.connect().execute('PRAGMA threads=%d' % multiprocessing.cpu_count())
            df = polars.from_arrow(con.execute(enhanced_exp).arrow())
            return self.quokka_context.from_polars(df)

        def duckdb_func(func, batch):
            batch_arrow = batch.to_arrow()
            for i, (col_name, type_) in enumerate(zip(batch_arrow.schema.names, batch_arrow.schema.types)):
                if pa.types.is_boolean(type_):
                    batch_arrow = batch_arrow.set_column(i, col_name, compute.cast(batch_arrow.column(col_name), pa.int32()))
            con = duckdb.connect().execute('PRAGMA threads=%d' % multiprocessing.cpu_count())
            return polars.from_arrow(con.execute(func).arrow())
        
        return self.quokka_context.new_stream(
            sources={0: self},
            partitioners={0: PassThroughPartitioner()},
            node=MapNode(
                schema=groupby + new_columns,
                schema_mapping={
                    **{new_column: {-1: new_column} for new_column in new_columns}, **{col: {0: col} for col in self.schema}},
                required_columns={0: required_columns},
                function=partial(duckdb_func, enhanced_exp),
                foldable=foldable),
            schema= groupby + new_columns,
            sorted = self.sorted
            )
    
    def union(self, other):

        """
        The union of two streams is a stream that contains all the elements of both streams.
        The two streams must have the same schema.
        Note: since DataStreams are not ordered, you should not make any assumptions on the ordering of the rows in the result.

        Args:
            other (DataStream): another DataStream of the same schema.

        Return:
            A new DataStream of the same schema as the two streams, with rows from both.

        Examples:

            >>> d = qc.read_csv("lineitem.csv")
            >>> d1 = qc.read_csv("lineitem1.csv")
            >>> d1 = d.union(d1)

        """

        assert self.schema == other.schema
        assert self.sorted == other.sorted

        class UnionExecutor(Executor):
            def __init__(self) -> None:
                self.state = None
            def execute(self,batches,stream_id, executor_id):
                return pa.concat_tables(batches)
            def done(self,executor_id):
                return
        
        executor = UnionExecutor()

        node = StatefulNode(
            schema=self.schema,
            # cannot push through any predicates or projections!
            schema_mapping={col: {0: col, 1: col} for col in self.schema},
            required_columns={0: set(), 1: set()},
            operator=executor
        )

        return self.quokka_context.new_stream(
            sources={0: self, 1: other},
            partitioners={0: PassThroughPartitioner(), 1: PassThroughPartitioner()},
            node=node,
            schema=self.schema,
            sorted=None
        )

    def gramian(self, columns):

        """
        This will compute DataStream[columns]^T * DataStream[columns]. The result will be len(columns) * len(columns), with schema same as columns.

        Args:
            columns (list): List of columns.

        Return:
            A new DataStream of shape len(columns) * len(columns) which is DataStream[columns]^T * DataStream[columns].

        Examples:

            >>> d = qc.read_csv("lineitem.csv")

            Now create two columns high and low using SQL.

            >>> d = d.gramian(["l_quantity", "l_extendedprice"])

            Result will be a 2x2 matrix.
        
        """

        for col in columns:
            assert col in self.schema

        if self.materialized:
            df = self._get_materialized_df()
            stuff = df.select(columns).to_numpy()
            product = np.dot(stuff.transpose(), stuff)
            return self.quokka_context.from_polars(polars.from_numpy(product, columns = columns))

        class AggExecutor(Executor):
            def __init__(self) -> None:
                self.state = None
            def execute(self,batches,stream_id, executor_id):
                for batch in batches:
                    #print(batch)
                    if self.state is None:
                        self.state = polars.from_arrow(batch)
                    else:
                        self.state += polars.from_arrow(batch)
            def done(self,executor_id):
                return self.state

        agg_executor = AggExecutor()
        def udf2(x):
            x = x.select(columns).to_numpy()
            product = np.dot(x.transpose(), x)
            return polars.from_numpy(product, columns = columns)

        stream = self.select(columns)
        stream = stream.transform( udf2, new_schema = columns, required_columns = set(columns), foldable=True)

        return stream.stateful_transform( agg_executor , columns, required_columns = set(columns),
                            partitioner=BroadcastPartitioner(), placement_strategy = SingleChannelStrategy())


    def with_columns_sql(self, new_columns: str, foldable = True):

        """
        This is the SQL analog of with_columns. 

        Args:
            new_columns (str): A SQL expression X as in 'SELECT *, X from DataStream'. You can specify multiple columns by separating them with commas.
                You must provide an alias for each column. Please look at the examples.
            foldable (bool): Whether or not the function can be executed as part of the batch post-processing of the previous operation in the
                execution graph. This is set to True by default. Correctly setting this flag requires some insight into how Quokka works. Lightweight

        Return:
            A new DataStream with new columns made by the user defined functions.

        Examples:

            >>> d = qc.read_csv("lineitem.csv")

            Now create two columns high and low using SQL.

            >>> d = d.with_columns_sql('o_orderpriority = "1-URGENT" or o_orderpriority = 2-HIGH as high, 
            ...                        o_orderpriority = "3-MEDIUM" or o_orderpriority = 4-NOT SPECIFIED" as low')

            Another example.

            >>> d = d.with_columns_sql('high + low as total')

            You must provide aliases for your columns, and separate the column defintiions with commas.
        
        """

        statements = new_columns.split(",")
        sql_statement = "select *, " + new_columns + " from batch_arrow"
        new_column_names = []
        required_columns = set()
        for statement in statements:
            node = sqlglot.parse_one(statement)
            assert type(node) == sqlglot.exp.Alias, "must provide new name for each column: x1 as some_compute, x2 as some_compute, etc."
            new_column_names.append(node.alias)
            required_columns = required_columns.union(required_columns_from_exp(node.this))

        def polars_func(batch):
            con = duckdb.connect().execute('PRAGMA threads=%d' % 8)
            batch_arrow = batch.to_arrow()
            return polars.from_arrow(con.execute(sql_statement).arrow())

        return self.quokka_context.new_stream(
            sources={0: self},
            partitioners={0: PassThroughPartitioner()},
            node=MapNode(
                schema=self.schema+ new_column_names,
                schema_mapping={
                    **{new_column: {-1: new_column} for new_column in new_column_names}, **{col: {0: col} for col in self.schema}},
                required_columns={0: required_columns},
                function=polars_func,
                foldable=foldable),
            schema=self.schema + new_column_names,
            sorted = self.sorted
            )

    def with_columns(self, new_columns: dict, required_columns=set(), foldable=True):

        """

        This will create new columns from certain columns in the dataframe. This is similar to Polars `with_columns`, Spark `with_columns`, etc. As usual,
        this function is not in-place, and will return a new DataStream, with the new column.
        
        This is a separate API from `transform` because the semantics allow for projection and predicate pushdown through this node, 
        since the original columns are all preserved. Use this instead of `transform` if possible.

        The arguments are a bit different from Polars `with_columns`. You need to specify a dictionary where key is new column name and value is either a Quokka
        Expression or a Python function (lambda function or regular function). I think this is better than the Polars way and removes the possibility of having 
        column names colliding.
        
        The preferred way is to supply Quokka Expressions for things that the Expression syntax supports. 
        In case a function is supplied, it must assume a single input, which is a Polars DataFrame. Please look at the examples. 

        A DataStream is implemented as a stream of batches. In the runtime, your function will be applied to each of those batches. The function must
        take as input a Polars DataFrame and produce a Polars DataFrame. This is a different mental model from Pandas or Polars `df.apply`, 
        where the function is written for each row. **The function's output must be a Polars Series (or DataFrame with one column)! **
        Of course you can call Polars or Pandas apply inside of this function if you have to do things row by row.
        
        You can use whatever libraries you have installed in your Python environment in this function. If you are using this on a
        cloud cluster, you have to make sure the necessary libraries are installed on each machine. You can use the `utils` package in pyquokka to help
        you do this, in particular you can use `mananger.install_python_package(cluster, package_name)`.
        
        Importantly, your function can take full advantage of Polars' columnar APIs to make use of SIMD and other forms of speedy goodness. 
        You can even use Polars LazyFrame abstractions inside of this function. Of course, for ultimate flexbility, you are more than welcome to convert 
        the Polars DataFrame to a Pandas DataFrame and use `df.apply`. Just remember to convert it back to a Polars DataFrame with only the result column in the end!

        Args:
            new_columns (dict): A dictionary of column names to UDFs or Expressions. The UDFs must take as input a Polars DataFrame and output a Polars DataFrame.
                The UDFs must not have expectations on the length of its input.
            required_columns (list or set): The names of the columns that are required for all your function. If this is not specified then Quokka assumes
                all the columns are required for your function. Early projection past this function becomes impossible. If you specify this and you got it wrong,
                you will get an error at runtime. This is only required for UDFs. If you use Quokka Expressions, then Quokka will automatically figure out the required columns.
            foldable (bool): Whether or not the function can be executed as part of the batch post-processing of the previous operation in the
                execution graph. This is set to True by default. Correctly setting this flag requires some insight into how Quokka works. Lightweight

        Return:
            A new DataStream with new columns made by the user defined functions.
        
        Examples:

            >>> d = qc.read_csv("lineitem.csv")

            You can use Polars APIs inside of custom lambda functions: 

            >>> d = d.with_columns({"high": lambda x:(x["o_orderpriority"] == "1-URGENT") | (x["o_orderpriority"] == "2-HIGH"), "low": lambda x:(x["o_orderpriority"] == "5-LOW") | (x["o_orderpriority"] == "4-NOT SPECIFIED")}, required_columns = {"o_orderpriority"})

            You can also use Quokka Expressions. You don't need to specify required columns if you use only Quokka Expressions:

            >>> d = d.with_columns({"high": (d["o_orderpriority"] == "1-URGENT") | (d["o_orderpriority"] == "2-HIGH"), "low": (d["o_orderpriority"] == "5-LOW") | (d["o_orderpriority"] == "4-NOT SPECIFIED")})
            
            Or mix the two. You then have to specify required columns again. It is the set of columns required for *all* your functions.

            >>> d = d.with_columns({"high": (lambda x:(x["o_orderpriority"] == "1-URGENT") | (x["o_orderpriority"] == "2-HIGH"), "low": (d["o_orderpriority"] == "5-LOW") | (d["o_orderpriority"] == "4-NOT SPECIFIED")}, required_columns = {"o_orderpriority"})
        """

        assert type(required_columns) == set

        # fix the new column ordering
        new_column_names = list(new_columns.keys())

        sql_statement = "select *"

        for new_column in new_columns:
            assert new_column not in self.schema, "For now new columns cannot have same names as existing columns"
            transform = new_columns[new_column]
            assert type(transform) == type(lambda x:1) or type(transform) == Expression, "Transform must be a function or a Quokka Expression"
            if type(transform) == Expression:
                required_columns = required_columns.union(transform.required_columns())
                sql_statement += ", " + transform.sql() + " as " + new_column 
                
            else:
                # detected an arbitrary function. If required columns are not specified, assume all columns are required
                if len(required_columns) == 0:
                    required_columns = set(self.schema)
        
        def polars_func(batch):

            con = duckdb.connect().execute('PRAGMA threads=%d' % 8)
            if sql_statement != "select *": # if there are any columns to add
                batch_arrow = batch.to_arrow()
                batch = polars.from_arrow(con.execute(sql_statement + " from batch_arrow").arrow())
            
            batch = batch.with_columns([polars.Series(name=column_name, values=new_columns[column_name](batch)) for column_name in new_column_names if type(new_columns[column_name]) == type(lambda x:1)])
            return batch

        return self.quokka_context.new_stream(
            sources={0: self},
            partitioners={0: PassThroughPartitioner()},
            node=MapNode(
                schema=self.schema+ new_column_names,
                schema_mapping={
                    **{new_column: {-1: new_column} for new_column in new_column_names}, **{col: {0: col} for col in self.schema}},
                required_columns={0: required_columns},
                function=polars_func,
                foldable=foldable),
            schema=self.schema + new_column_names,
            sorted = self.sorted
            )

    def stateful_transform(self, executor: Executor, new_schema: list, required_columns: set,
                           partitioner=PassThroughPartitioner(), placement_strategy = CustomChannelsStrategy(1)):

        """

        **EXPERIMENTAL API** 

        This is like `transform`, except you can use a stateful object as your transformation function. This is useful for example, if you want to run
        a heavy Pytorch model on each batch coming in, and you don't want to reload this model for each function call. Remember the `transform` API only
        supports stateless transformations. You could also implement much more complicated stateful transformations, like implementing your own aggregation
        function if you are not satisfied with Quokka's default operator's performance.

        This API is still being finalized. A version of it that takes multiple input streams is also going to be added. This is the part of the DataStream level 
        api that is closest to the underlying execution engine. Quokka's underlying execution engine basically executes a series of stateful transformations
        on batches of data. The difficulty here is how much of that underlying API to expose here so it's still useful without the user having to understand 
        how the Quokka runtime works. To that end, we have to come up with suitable partitioner and placement strategy abstraction classes and interfaces.

        If you are interested in helping us hammer out this API, please talke to me: zihengw@stanford.edu.

        Args:
            executor (pyquokka.executors.Executor): The stateful executor. It must be a subclass of `pyquokka.executors.Executor`, and expose the `execute` 
                and `done` functions. More details forthcoming.
            new_schema (list): The names of the columns of the Polars DataFrame that the transformation function produces. 
            required_columns (list or set): The names of the columns that are required for this transformation. This argument is made mandatory
                because it's often trivial to supply and can often greatly speed things up.

        Return:
            A transformed DataStream.
        
        Examples:
            Check the code for the `gramian` function.
        """

        assert type(required_columns) == set
        assert issubclass(type(executor), Executor), "user defined executor must be an instance of a \
            child class of the Executor class defined in pyquokka.executors. You must override the execute and done methods."

        select_stream = self.select(required_columns)

        custom_node = StatefulNode(
            schema=new_schema,
            # cannot push through any predicates or projections!
            schema_mapping={col: {-1: col} for col in new_schema},
            required_columns={0: required_columns},
            operator=executor
        )

        custom_node.set_placement_strategy(placement_strategy)

        return self.quokka_context.new_stream(
            sources={0: select_stream},
            partitioners={0: partitioner},
            node=custom_node,
            schema=new_schema,
            
        )
    
    def distinct(self, keys: list):

        """
        Return a new DataStream with specified columns and unique rows. This is like `SELECT DISTINCT(KEYS) FROM ...` in SQL.

        Note all the other columns will be dropped, since their behavior is unspecified. If you want to do deduplication, you can use
        this operator with keys set to all the columns.

        This could be accomplished by using `groupby().agg()` but using `distinct` is generally faster because it is nonblocking, 
        compared to a groupby. Quokka really likes nonblocking operations because it can then pipeline it with other operators.

        Args:
            keys (list): a list of columns to select distinct on.

        Return:
            A transformed DataStream whose columns are in keys and whose rows are unique.
        
        Examples:
            
            >>> f = qc.read_csv("lineitem.csv")

            Select only the l_orderdate and l_orderkey columns, return only unique rows.
            
            >>> f = f.distinct(["l_orderdate", "l_orderkey"])

            This will now fail, since l_comment is no longer in f's schema.

            >>> f = f.select(["l_comment"])
        """

        if type(keys) == str:
            keys = [keys]
        assert type(keys) == list, "keys must be a list of column names"
        assert all([key in self.schema for key in keys]), "keys must be a subset of the columns in the DataStream"

        select_stream = self.select(keys)

        return self.quokka_context.new_stream(
            sources={0: select_stream},
            partitioners={0: HashPartitioner(keys[0])},
            node=StatefulNode(
                schema=keys,
                # this is a stateful node, but predicates and projections can be pushed down.
                schema_mapping={col: {0: col} for col in keys},
                required_columns={0: set(keys)},
                operator=DistinctExecutor(keys)
            ),
            schema=keys,
            
        )

    def join(self, right, on=None, left_on=None, right_on=None, suffix="_2", how="inner", maintain_sort_order=None):

        """
        Join a DataStream with another DataStream. This may result in a distributed hash join or a broadcast join depending on cardinality estimates.



        

        Args:
            right (DataStream): the DataStream to join to.
            on (str): You could either specify this, if the join column has the same name in this DataStream and `right`, or `left_on` and `right_on` 
                if the join columns don't have the same name.
            left_on (str): the name of the join column in this DataStream.
            right_on (str): the name of the join column in `right`.
            suffix (str): if `right` has columns with the same names as columns in this DataStream, their names will be appended with the suffix in the result.
            how (str): supports "inner", "left", "semi" or "anti"

        Return:
            A new DataStream that's the joined result of this DataStream and "right". By default, columns from both side will be retained, 
            except for `right_on` from the right side. 
        
        Examples:

            >>> lineitem = qc.read_csv("lineitem.csv")

            >>> orders = qc.read_csv("orders.csv")

            >>> result = lineitem.join(orders, left_on = "l_orderkey", right_on = "o_orderkey")

            >>> result = result.select(["o_orderkey"])
        """

        assert how in {"inner", "left", "semi", "anti"}
        assert issubclass(type(right), DataStream), "must join against a Quokka DataStream"

        if maintain_sort_order is not None:

            assert how in {"inner", "left"}

            # our broadcast join strategy should automatically satisfy this, no need to do anything special
            if type(right) == polars.internals.DataFrame:
                assert maintain_sort_order == "left"
                assert self.sorted is not None
            
            else:
                assert maintain_sort_order in {"left", "right"}
                if maintain_sort_order == "left":
                    assert self.sorted is not None
                else:
                    assert right.sorted is not None
                if how == "left":
                    assert maintain_sort_order == "right", "in a left join, can only maintain order of the right table"
        
        #if type(right) == polars.internals.DataFrame and right.to_arrow().nbytes > 10485760:
        #    raise Exception("You cannot join a DataStream against a Polars DataFrame more than 10MB in size. Sorry.")

        if on is None:
            assert left_on is not None and right_on is not None
            assert left_on in self.schema, "join key not found in left table"
            assert right_on in right.schema, "join key not found in right table"
        else:
            assert on in self.schema, "join key not found in left table"
            assert on in right.schema, "join key not found in right table"
            left_on = on
            right_on = on
            on = None

        # we can't do this check since schema is now a list of names with no type info. This should change in the future.
        #assert node1.schema[left_on] == node2.schema[right_on], "join column has different schema in tables"

        new_schema = self.schema.copy()
        if self.materialized:
            schema_mapping = {col: {-1: col} for col in self.schema}
        else:
            schema_mapping = {col: {0: col} for col in self.schema}

        # if the right table is already materialized, the schema mapping should forget about it since we can't push anything down anyways.
        # an optimization could be to push down the predicate directly to the materialized Polars DataFrame in the BroadcastJoinExecutor
        # leave this as a TODO. this could be greatly benenficial if it significantly reduces the size of the small table.
        if right.materialized:
            right_table_id = -1
        else:
            right_table_id = 1

        rename_dict = {}

        # import pdb;pdb.set_trace()

        right_cols = right.schema if how not in {"semi", "anti"} else [right_on]
        for col in right_cols:
            if col == right_on:
                continue
            if col in new_schema:
                assert col + \
                    suffix not in new_schema, (
                        "the suffix was not enough to guarantee unique col names", col + suffix, new_schema)
                new_schema.append(col + suffix)
                schema_mapping[col+suffix] = {right_table_id: col + suffix}
                rename_dict[col] = col + suffix
            else:
                new_schema.append(col)
                schema_mapping[col] = {right_table_id: col}
        
        # you only need the key column on the RHS! select overloads in DataStream or Polars DataFrame runtime polymorphic
        if how == "semi" or how == "anti":
            right = right.select([right_on])
        
        if len(rename_dict) > 0:
            right = right.rename(rename_dict)

        if (not self.materialized and not right.materialized) or (self.materialized and not right.materialized and how != "inner"):

            # if self.materialized, rewrite the schema_mapping
            for col in schema_mapping:
                if list(schema_mapping[col].keys())[0] == -1:
                    schema_mapping[col] = {0: col}

            if maintain_sort_order is None:
                assume_sorted = {}
            elif maintain_sort_order == "left":
                assume_sorted = {0: True}
            else:
                assume_sorted = {1: True}
            
            return self.quokka_context.new_stream(
                sources={0: self, 1: right},
                partitioners={0: HashPartitioner(
                    left_on), 1: HashPartitioner(right_on)},
                node=JoinNode(
                    schema=new_schema,
                    schema_mapping=schema_mapping,
                    required_columns={0: {left_on}, 1: {right_on}},
                    join_spec=(how, {0: left_on, 1: right_on}),
                    assume_sorted=assume_sorted),
                schema=new_schema,
                )

        elif self.materialized and not right.materialized:

            assert how in {"inner"}

            new_schema.remove(left_on)
            new_schema = [right_on] + new_schema
            del schema_mapping[left_on]
            schema_mapping[right_on] = {1: right_on}
                        
            new_stream = self.quokka_context.new_stream(
                sources={1: right},
                partitioners={1: PassThroughPartitioner()},
                node=BroadcastJoinNode(
                    schema=new_schema,
                    schema_mapping=schema_mapping,
                    required_columns={1: {right_on}},
                    operator=BroadcastJoinExecutor(
                        self._get_materialized_df(), small_on=left_on, big_on=right_on, suffix=suffix, how=how)
                ),
                schema=new_schema,
                )
            if right_on == left_on:
                return new_stream
            else:
                return new_stream.rename({right_on: left_on})

        elif not self.materialized and right.materialized:
            
            return self.quokka_context.new_stream(
                sources={0: self},
                partitioners={0: PassThroughPartitioner()},
                node=BroadcastJoinNode(
                    schema=new_schema,
                    schema_mapping=schema_mapping,
                    required_columns={0: {left_on}},
                    operator=BroadcastJoinExecutor(
                        right._get_materialized_df(), small_on=right_on, big_on=left_on, suffix=suffix, how=how)
                ),
                schema=new_schema,
                )

        else:

            right_df = right._get_materialized_df()
            left_df = self._get_materialized_df()
            result = left_df.join(right_df, how=how, left_on=left_on, right_on=right_on, suffix=suffix)
            return self.quokka_context.from_polars(result)

    def groupby(self, groupby: list, orderby=None):

        """
        Group a DataStream on a list of columns, optionally specifying an ordering requirement.

        This returns a GroupedDataStream object, which currently only expose the `aggregate` method. This is similar to Pandas `df.groupby().agg()` syntax.
        Eventually the GroupedDataStream object will also support different kinds of window functions. 

        Args:
            groupby (list or str): a column or a list of columns to group on.
            orderby (list): a list of ordering requirements of the groupby columns, specified in a list like this:
                [(col1, "asc"), (col2, "desc")]. 

        Return:
            A GroupedDataStream object with the specified grouping and the current DataStream.
        
        Examples:

            >>> lineitem = qc.read_csv("lineitem.csv")

            `result` will be a GroupedDataStream.

            >>> result = lineitem.groupby(["l_orderkey","l_orderdate"], orderby = [("l_orderkey", "asc"), ("l_orderdate", "desc")])
            
        """

        if type(groupby) == str:
            groupby = [groupby]

        assert type(groupby) == list and len(
            groupby) > 0, "must specify at least one group key as a list of group keys, i.e. [key1,key2]"
        if orderby is not None:
            assert type(orderby) == list
            for i in range(len(orderby)):
                if type(orderby[i]) == tuple:
                    assert orderby[i][0] in groupby
                    assert orderby[i][1] == "asc" or orderby[i][1] == "desc"
                elif type(orderby[i]) == str:
                    assert orderby[i] in groupby
                    orderby[i] = (orderby[i], "asc")
                else:
                    raise Exception("don't understand orderby format")

        return GroupedDataStream(self, groupby=groupby, orderby=orderby)
    
    def windowed_transform(self, window: Window, trigger: Trigger):

        """
        This is a helper function for `windowed_aggregate` and `windowed_aggregate_with_state`. It is not meant to be used directly.
        aggregations should be a list of polars expressions.
        """

        time_col = window.order_by
        by_col = window.partition_by

        assert self.sorted is not None, "DataStream must be sorted before windowed aggregation."
        assert time_col in self.sorted and self.sorted[time_col] == "stride"

        required_columns = window.get_required_cols()
        new_schema = [time_col, by_col] + list(window.get_new_cols())

        assert type(required_columns) == set

        required_columns.add(time_col)
        required_columns.add(by_col)
        select_stream = self.select(required_columns)

        if issubclass(type(window), HoppingWindow):
            operator = HoppingWindowExecutor(
                time_col, by_col, window, trigger)
        elif issubclass(type(window), SlidingWindow):
            operator = SlidingWindowExecutor(
                time_col, by_col, window, trigger)
        elif issubclass(type(window), SessionWindow):
            operator = SessionWindowExecutor(
                time_col, by_col, window, trigger)
        else:
            raise Exception

        node = StatefulNode(
                schema=new_schema,
                # cannot push through any predicates or projections!
                schema_mapping={col: {-1: col} for col in new_schema},
                required_columns={0: required_columns},
                operator=operator,
                assume_sorted={0:True}
            )
        
        node.set_output_sorted_reqs({time_col: ("sorted_within_key", by_col)})

        return self.quokka_context.new_stream(
            sources={0: select_stream},
            partitioners={0: HashPartitioner(by_col)},
            node=node,
            schema=new_schema,
        )

    def top_k(self, columns, k, descending = None):
        """
        This is a topk function that effectively performs select * from stream order by columns limit k.
        The strategy is to take k rows from each batch coming in and do a final sort and limit k in a stateful executor.

        Args:
            columns (str or list): a column or a list of columns to sort on.
            k (int): the number of rows to return.
            descending (bool or list): a boolean or a list of booleans indicating whether to sort in descending order. If a list, the length must be the same as the length of `columns`.

        Return:
            A DataStream object with the specified top k rows.

        Examples:

            >>> lineitem = qc.read_csv("lineitem.csv")

            `result` will be a DataStream.

            >>> result = lineitem.top_k("l_orderkey", 10)
            >>> result = lineitem.top_k(["l_orderkey", "l_orderdate"], 10, descending = [True, False])
        """
        if type(columns) == str:
            columns = [columns]
        assert type(columns) == list and len(columns) > 0
        
        if descending is not None:
            if type(descending) == bool:
                descending = [descending]
            assert type(descending) == list and len(descending) == len(columns)
            assert all([type(i) == bool for i in descending])
        else:
            descending = [False] * len(columns)
        
        assert type(k) == int
        assert k > 0

        new_columns = []
        for i in range(len(columns)):
            if descending[i]:
                new_columns.append(columns[i] + " desc")
            else:
                new_columns.append(columns[i] + " asc")

        sql_statement = "select * from batch_arrow order by " + ",".join(new_columns) + " limit " + str(k)

        def f(df):
            batch_arrow = df.to_arrow()
            con = duckdb.connect().execute('PRAGMA threads=%d' % 8)
            return polars.from_arrow(con.execute(sql_statement).arrow())
        
        transformed = self.transform(f, new_schema = self.schema, required_columns=set(self.schema))
        
        topk_node = StatefulNode(
            schema=self.schema,
            schema_mapping={col: {0: col} for col in self.schema},
            required_columns={0: set(columns)},
            operator=ConcatThenSQLExecutor(sql_statement)
        )
        topk_node.set_placement_strategy(SingleChannelStrategy())
        return self.quokka_context.new_stream(
            sources={0: transformed},
            partitioners={0: BroadcastPartitioner()},
            node=topk_node,
            schema=self.schema,
        )
    
    def _grouped_count_distinct(self, groupby: list, count_col: str, orderby: list = None):

        assert type(groupby) == list
        assert type(count_col) == str
        new_schema = [count_col]

        if len(groupby) == 0:
            sql_statement = "select count(distinct {}) as {} from batch_arrow".format(count_col, count_col)
        else:
            sql_statement = "select {}, count(distinct {}) as {} from batch_arrow group by {}".format(
                ",".join(groupby), count_col, count_col, ",".join(groupby)
            )

        if orderby is not None:
            assert type(orderby) == list
            sql_statement += " order by "
            for i in range(len(orderby)):
                assert type(orderby[i]) == tuple
                key = orderby[i][0]
                direction = orderby[i][1]
                sql_statement += "{} {},".format(key, direction)
            sql_statement = sql_statement[:-1]

        agg_node = StatefulNode(
            schema=groupby + new_schema,
            schema_mapping={
                    **{new_column: {-1: new_column} for new_column in new_schema}, **{col: {0: col} for col in groupby}},
            required_columns={0: set(groupby + [count_col])},
            operator=ConcatThenSQLExecutor(sql_statement),
        )
        
        if len(groupby) > 0:
            aggregated_stream = self.quokka_context.new_stream(
                sources={0: self},
                partitioners={0: HashPartitioner(groupby[0])},
                node=agg_node,
                schema=groupby + new_schema,
            )
        else:
            agg_node.set_placement_strategy(SingleChannelStrategy())
            aggregated_stream = self.quokka_context.new_stream(
                sources={0: self},
                partitioners={0: BroadcastPartitioner()},
                node=agg_node,
                schema=groupby + new_schema,
                
            )
        return aggregated_stream

    
    def _grouped_aggregate_sql(self, groupby: list, aggregations: str, orderby = None):

        try:
            batch_agg, final_agg, new_schema = sql_utils.parse_multiple_aggregations(aggregations)
        except Exception as e:
            raise Exception("Error parsing aggregations: " + str(e))
    
        clauses = aggregations.split(",")
        assert all(["as" in clause or "AS" in clause for clause in clauses]), "must provide alias for each aggregation"

        agged = self.transform_sql(batch_agg, groupby)

        # now we need to groupby and aggregate the final_agg
        agg_node = StatefulNode(
            schema=groupby + new_schema,
            schema_mapping={
                    **{new_column: {-1: new_column} for new_column in new_schema}, **{col: {0: col} for col in groupby}},
            required_columns={0: set(agged.schema)},
            operator=SQLAggExecutor(groupby, orderby, final_agg)
        )
        if len(groupby) > 0:
            aggregated_stream = self.quokka_context.new_stream(
                sources={0: agged},
                partitioners={0: HashPartitioner(groupby[0])},
                node=agg_node,
                schema=groupby + new_schema,
                
            )
        else:
            agg_node.set_placement_strategy(SingleChannelStrategy())
            aggregated_stream = self.quokka_context.new_stream(
                sources={0: agged},
                partitioners={0: BroadcastPartitioner()},
                node=agg_node,
                schema=groupby + new_schema,
                
            )
        return aggregated_stream

    def _grouped_aggregate(self, groupby: list, aggregations: dict, orderby=None):
        # we are going to convert the aggregations_dict into a SQL statement and call _grouped_aggregate_sql

        # first, we need to convert the aggregations dict into a SQL statement
        sql = ""
        for col, agg in aggregations.items():
            if col == "*":
                assert agg == "count" or agg == ["count"]
                sql += f"count(*) as count,"
                continue
            if type(agg) == str:
                agg = [agg]
            for a in agg:
                if a == "min":
                    sql += f"min({col}) as {col}_min,"
                elif a == "max":
                    sql += f"max({col}) as {col}_max,"
                elif a == "mean":
                    sql += f"avg({col}) as {col}_mean,"
                elif a == "sum":
                    sql += f"sum({col}) as {col}_sum,"
                elif a == "avg":
                    sql += f"avg({col}) as {col}_avg,"
                else:
                    raise Exception("Unrecognized aggregation: " + a)
        sql = sql[:-1]
        return self._grouped_aggregate_sql(groupby, sql, orderby)

    def count_distinct(self, col):

        """
        Count the number of distinct values of a column. This may result in out of memory. This is not approximate.

        Args:
            col (str): the column to count distinct values of
        
        """

        return self._grouped_count_distinct([], col)

    def agg(self, aggregations):

        """
        Aggregate this DataStream according to the defined aggregations without any pre-grouping. This is similar to Pandas `df.agg()`.
        The result will be one row.

        The result is a DataStream that will return a batch when the entire aggregation is done, since it's impossible to return any aggregation
        results without seeing the entire dataset. As a result, you should call `.compute()` or `.collect()` on this DataStream instead of doing 
        additional operations on it like `.filter()` since those won't be pipelined anyways. The only reason Quokka by default returns a DataStream
        instead of just returning a Polars DataFrame or a Quokka DataSet is so you can do `.explain()` on it.

        Args:
            aggregations (dict): similar to a dictionary argument to Pandas `df.agg()`. The key is the column name, where the value
                is a str that is "min", "max", "mean", "sum", "avg" or a list of such strings. If you desire to have the count column
                in your result, add a key "*" with value "count". Look at the examples.

        Return:
            A DataStream object that holds the aggregation result. It will only emit one batch, which is the result when it's done. 
        
        Examples:
            
            >>> lineitem = qc.read_csv("lineitem.csv")
            
            >>> d = lineitem.filter("l_shipdate <= date '1998-12-01' - interval '90' day")
            
            >>> d = d.with_column("disc_price", lambda x:x["l_extendedprice"] * (1 - x["l_discount"]), required_columns ={"l_extendedprice", "l_discount"})
            
            I want the sum and average of the l_quantity column and the l_extendedprice colum, the sum of the disc_price column, the minimum of the l_discount
            column, and oh give me the total row count as well.
            
            >>> f = d.agg({"l_quantity":["sum","avg"], "l_extendedprice":["sum","avg"], "disc_price":"sum", "l_discount":"min","*":"count"})
            
        """

        return self._grouped_aggregate([], aggregations, None)

    def agg_sql(self, aggregations: str):

        """
        This is the SQL version of `agg`. It takes a SQL statement as input instead of a dictionary. The SQL statement must be a valid SQL statement.
        The requirements are similar to what you need for `transform_sql`. Please look at the examples. Exotic SQL statements may not work, such as `count_distinct`, `percentile` etc.
        Please limit your aggregations to mean/max/min/sum/avg/count for now. 

        Args:
            aggregations (str): a valid SQL statement. The requirements are similar to what you need for `transform_sql`. 
        
        Return:
            A DataStream object that holds the aggregation result. It will only emit one batch, which is the result when it's done.
        
        Examples:

            >>> d = d.agg_sql("sum(l_extendedprice * (1 - l_discount)) as revenue")

            >>> f = d.agg_sql("count(*) as count_order")

            >>>  f = d.agg_sql("
            >>>        sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,
            >>>        sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
            >>>    ")
        
        """

        return self._grouped_aggregate_sql([], aggregations, None)

    def aggregate(self, aggregations):

        """
        Alias of `agg`.
        """

        return self.agg(aggregations)
    
    def count(self):

        """
        Return total row count.
        """

        return self.agg({"*":"count"}).collect()

    def sum(self, columns):

        """
        Return the sums of the specified columns.

        Args:
            columns (str or list): the column name or a list of column names to sum.
        """

        assert type(columns) == str or type(columns) == list
        if type(columns) == str:
            columns = [columns]
        for col in columns:
            assert col in self.schema
        return self.agg({col: "sum" for col in columns}).collect()
    
    def max(self, columns):

        """
        Return the maximum values of the specified columns.

        Args:
            columns (str or list): the column name or a list of column names.
        """

        assert type(columns) == str or type(columns) == list
        if type(columns) == str:
            columns = [columns]
        for col in columns:
            assert col in self.schema
        return self.agg({col: "max" for col in columns}).collect()

    def min(self, columns):

        """
        Return the minimum values of the specified columns.

        Args:
            columns (str or list): the column name or a list of column names.
        """

        assert type(columns) == str or type(columns) == list
        if type(columns) == str:
            columns = [columns]
        for col in columns:
            assert col in self.schema
        return self.agg({col: "min" for col in columns}).collect()
    
    def mean(self, columns):

        """
        Return the mean values of the specified columns.

        Args:
            columns (str or list): the column name or a list of column names.
        """

        assert type(columns) == str or type(columns) == list
        if type(columns) == str:
            columns = [columns]
        for col in columns:
            assert col in self.schema
        return self.agg({col: "mean" for col in columns}).collect()


class GroupedDataStream:
    def __init__(self, source_data_stream: DataStream, groupby, orderby) -> None:
        
        self.source_data_stream = source_data_stream
        self.groupby = groupby if type(groupby) == list else [groupby]
        self.orderby = orderby
    
    def cogroup(self, right, executor: Executor, new_schema: list, required_cols_left = None, required_cols_right = None):
        """
        Purely Experimental API.
        """

        assert (type(right) == GroupedDataStream or type(right) == polars.internals.DataFrame) and issubclass(type(executor), Executor)
        
        assert len(self.groupby) == 1 and len(right.groupby) == 1, "we only support single key partition functions right now"
        assert self.groupby[0] == right.groupby[0], "must be grouped by the same key"
        copartitioner = self.groupby[0]

        schema_mapping={col: {-1: col} for col in new_schema}

        if required_cols_left is None:
            required_cols_left = set(self.source_data_stream.schema)
        else:
            if type(required_cols_left) == list:
                required_cols_left = set(required_cols_left)
            assert type(required_cols_left) == set
        
        if required_cols_right is None:
            required_cols_right = set(right.source_data_stream.schema)
        else:
            if type(required_cols_right) == list:
                required_cols_right = set(required_cols_right)
            assert type(required_cols_right) == set

        if type(right) == GroupedDataStream:

            return self.source_data_stream.quokka_context.new_stream(
                sources={0: self.source_data_stream, 1: right.source_data_stream},
                partitioners={0: HashPartitioner(
                    copartitioner), 1: HashPartitioner(copartitioner)},
                node=StatefulNode(
                    schema=new_schema,
                    schema_mapping=schema_mapping,
                    required_columns={0: required_cols_left, 1: required_cols_right},
                    operator= executor),
                schema=new_schema,
                )

        elif type(right) == polars.internals.DataFrame:
            
            raise NotImplementedError

    def count_distinct(self, col: str):

        """
        Count the number of distinct values of a column for each group. This may result in out of memory. This is not approximate.

        Args:
            col (str): the column to count distinct values of
        
        """

        return self.source_data_stream._grouped_count_distinct(self.groupby, col, self.orderby)

    def agg(self, aggregations: dict):

        """
        Aggregate this GroupedDataStream according to the defined aggregations. This is similar to Pandas `df.groupby().agg()`.
        The result's length will be however number of rows as there are unique group keys combinations.

        The result is a DataStream that will return a batch when the entire aggregation is done, since it's impossible to return any aggregation
        results without seeing the entire dataset. As a result, you should call `.compute()` or `.collect()` on this DataStream instead of doing 
        additional operations on it like `.filter()` since those won't be pipelined anyways. The only reason Quokka by default returns a DataStream
        instead of just returning a Polars DataFrame or a Quokka DataSet is so you can do `.explain()` on it.

        Args:
            aggregations (dict): similar to a dictionary argument to Pandas `df.agg()`. The key is the column name, where the value
                is a str that is "min", "max", "mean", "sum", "avg" or a list of such strings. If you desire to have the count column
                in your result, add a key "*" with value "count". Look at the examples.

        Return:
            A DataStream object that holds the aggregation result. It will only emit one batch, which is the result when it's done. 
            You should call `.collect()` or `.compute()` on it as it is impossible to pipeline past an 
            aggregation, so might as well as materialize it right now.
        
        Examples:
            
            >>> lineitem = qc.read_csv("lineitem.csv")
            
            >>> d = lineitem.filter("l_shipdate <= date '1998-12-01' - interval '90' day")
            
            >>> d = d.with_column("disc_price", lambda x:x["l_extendedprice"] * (1 - x["l_discount"]), required_columns ={"l_extendedprice", "l_discount"})
            
            I want the sum and average of the l_quantity column and the l_extendedprice colum, the sum of the disc_price column, the minimum of the l_discount
            column, and oh give me the total row count as well, of each unique combination of l_returnflag and l_linestatus
            
            >>> f = d.groupby(["l_returnflag", "l_linestatus"]).agg({"l_quantity":["sum","avg"], "l_extendedprice":["sum","avg"], "disc_price":"sum", "l_discount":"min","*":"count"})
        """

        return self.source_data_stream._grouped_aggregate(self.groupby, aggregations, self.orderby)

    def agg_sql(self, aggregations: str):

        """
        The SQL version of `agg`. Look at the examples.

        Args:
            aggregations (str): a string that is a valid SQL aggregation expression. Look at the examples.

        Return:
            A DataStream object that holds the aggregation result. It will only emit one batch, which is the result when it's done.

        Examples:

            >>> d = d.groupby(["l_orderkey","o_orderdate","o_shippriority"]).agg_sql("sum(l_extendedprice * (1 - l_discount)) as revenue")

            >>> f = d.groupby("o_orderpriority").agg_sql("count(*) as count_order")

            >>>  f = d.groupby("l_shipmode").agg_sql("
            >>>        sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,
            >>>        sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
            >>>    ")

        """

        return self.source_data_stream._grouped_aggregate_sql(self.groupby, aggregations, self.orderby)

    def aggregate(self, aggregations: dict):
        """
        Alias for agg.
        """
        return self.agg(aggregations)
