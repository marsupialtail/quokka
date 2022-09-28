class OutputS3CSVExecutor(Executor):
    def __init__(self, bucket, prefix, output_line_limit = 1000000) -> None:
        self.num = 0

        self.bucket = bucket
        self.prefix = prefix
        self.output_line_limit = output_line_limit
        self.name = 0
        self.my_batches = deque()
        self.exe = None
        self.executor_id = None
        self.session = get_session()

    def serialize(self):
        return {}, "all"
    
    def deserialize(self, s):
        pass

    def create_csv_file(self, data):
        da = BytesIO()
        csv.write_csv(data.to_arrow(), da,  write_options = csv.WriteOptions(include_header=False))
        return da

    async def do(self, client, data, name):
        da = await asyncio.get_running_loop().run_in_executor(self.exe, self.create_csv_file, data)
        resp = await client.put_object(Bucket=self.bucket,Key=self.prefix + "-" + str(self.executor_id) + "-" + str(name) + ".csv",Body=da.getvalue())
        #print(resp)

    async def go(self, datas):
        # this is expansion of async with, so you don't have to remake the client
        
        async with self.session.create_client("s3", region_name="us-west-2") as client:
            todos = []
            for i in range(len(datas)):
                todos.append(self.do(client,datas[i], self.name))
                self.name += 1
        
            await asyncio.gather(*todos)
        
    def execute(self,batches,stream_id, executor_id):

        if self.exe is None:
            self.exe = concurrent.futures.ThreadPoolExecutor(max_workers = 2)
        if self.executor_id is None:
            self.executor_id = executor_id
        else:
            assert self.executor_id == executor_id

        self.my_batches.extend([i for i in batches if i is not None])
        #print("MY OUTPUT CSV STATE", [len(i) for i in self.my_batches] )

        curr_len = 0
        i = 0
        datas = []
        process = psutil.Process(os.getpid())
        print("mem usage output", process.memory_info().rss, pa.total_allocated_bytes())
        while i < len(self.my_batches):
            curr_len += len(self.my_batches[i])
            #print(curr_len)
            i += 1
            if curr_len > self.output_line_limit:
                #print("writing")
                datas.append(polars.concat([self.my_batches.popleft() for k in range(i)], rechunk = True))
                i = 0
                curr_len = 0
        #print("writing ", len(datas), " files")
        asyncio.run(self.go(datas))
        #print("mem usage after", process.memory_info().rss, pa.total_allocated_bytes())

    def done(self,executor_id):
        if len(self.my_batches) > 0:
            datas = [polars.concat(list(self.my_batches), rechunk=True)]
            asyncio.run(self.go(datas))
        print("done")

class OutputS3ParquetFastExecutor(Executor):
    def __init__(self, bucket, prefix, output_line_limit = 10000000) -> None:
        self.num = 0
        self.bucket = bucket
        self.prefix = prefix
        self.output_line_limit = output_line_limit
        self.name = 0
        self.my_batches = deque()
        self.exe = None
        self.executor_id = None
        self.client = None

    def serialize(self):
        return {}, "all"
    
    def deserialize(self, s):
        pass

    def create_parquet_file(self, data, name):
        writer = pa.BufferOutputStream()
        pq.write_table(data.to_arrow(), writer)
        resp = self.client.put_object(Bucket=self.bucket,Key=self.prefix + "-" + str(self.executor_id) + "-" + str(name) + ".parquet",Body=bytes(writer.getvalue()))
        return resp

    async def do(self, data,  name):
        da = await asyncio.get_running_loop().run_in_executor(self.exe, self.create_parquet_file, data, name)
        print(da)

    async def go(self, datas):
        # this is expansion of async with, so you don't have to remake the client
        todos = []
        for i in range(len(datas)):
            todos.append(self.do(datas[i], self.name))
            self.name += 1
    
        await asyncio.gather(*todos)
        
    def execute(self,batches,stream_id, executor_id):

        if self.exe is None:
            self.exe = concurrent.futures.ThreadPoolExecutor(max_workers = 8)
        if self.executor_id is None:
            self.executor_id = executor_id
        else:
            assert self.executor_id == executor_id
        
        if self.client is None:
            self.client = boto3.client("s3")

        print("LEN BATCHES", len(batches))
        self.my_batches.extend([i for i in batches if i is not None])
        #print("MY OUTPUT CSV STATE", [len(i) for i in self.my_batches] )

        curr_len = 0
        i = 0
        datas = []
        process = psutil.Process(os.getpid())
        #print("mem usage output", process.memory_info().rss, pa.total_allocated_bytes())
        while i < len(self.my_batches):
            curr_len += len(self.my_batches[i])
            #print(curr_len)
            i += 1
            if curr_len > self.output_line_limit:
                #print("writing")
                datas.append(polars.concat([self.my_batches.popleft() for k in range(i)], rechunk = True))
                i = 0
                curr_len = 0
        #print("writing ", len(datas), " files")
        asyncio.run(self.go(datas))
        #print("mem usage after", process.memory_info().rss, pa.total_allocated_bytes())

    def done(self,executor_id):
        if len(self.my_batches) > 0:
            datas = [polars.concat(list(self.my_batches), rechunk=True)]
            asyncio.run(self.go(datas))
        print("done")

# WARNING: this is currently extremely inefficient. But it is indeed very general.
# pyarrow joins do not support list data types 
class GenericGroupedAggExecutor(Executor):
    def __init__(self, funcs, final_func = None):

        # how many things you might checkpoint, the number of keys in the dict

        self.state = None
        self.funcs = funcs # this will be a dictionary of col name -> tuple of (merge func, fill_value)
        self.cols = list(funcs.keys())
        self.final_func = final_func

    def serialize(self):
        return {0:self.state}, "all"
    
    def deserialize(self, s):
        # the default is to get a list of dictionaries.
        assert type(s) == list and len(s) == 1
        self.state = s[0][0]
    
    def execute(self,batches, stream_id, executor_id):
        batches = [i for i in batches if i is not None]
        for batch in batches:
            assert type(batch) == pd.core.frame.DataFrame # polars add has no index, will have wierd behavior
            if self.state is None:
                self.state = batch 
            else:
                self.state = self.state[self.cols].join(batch[self.cols],lsuffix="_bump",how="outer")
                for col in self.cols:
                    func, fill_value = self.funcs[col]
                    isna = self.state[col].isna()
                    self.state.loc[isna, [col]] = pd.Series([fill_value] * isna.sum()).values
                    isna = self.state[col + "_bump"].isna()
                    self.state.loc[isna, [col + "_bump"]] = pd.Series([fill_value] * isna.sum()).values
                    self.state[col] = func(self.state[col], self.state[col + "_bump"])
                    self.state.drop(columns = [col+"_bump"], inplace=True)
                    
    
    def done(self,executor_id):
        if self.final_func:
            return self.final_func(self.state)
        else:
            #print(self.state)
            return self.state

class AddExecutor(Executor):
    def __init__(self, fill_value = 0, final_func = None):

        # how many things you might checkpoint, the number of keys in the dict
        self.num_states = 1

        self.state = None
        self.fill_value = fill_value
        self.final_func = final_func

    def serialize(self):
        return {0:self.state}, "all"
    
    def deserialize(self, s):
        # the default is to get a list of dictionaries.
        assert type(s) == list and len(s) == 1
        self.state = s[0][0]
    
    # the execute function signature does not change. stream_id will be a [0 - (length of InputStreams list - 1)] integer
    def execute(self,batches, stream_id, executor_id):
        batches = [i for i in batches if i is not None]
        for batch in batches:
            assert type(batch) == pd.core.frame.DataFrame # polars add has no index, will have wierd behavior
            if self.state is None:
                self.state = batch 
            else:
                self.state = self.state.add(batch, fill_value = self.fill_value)
    
    def done(self,executor_id):
        if self.final_func:
            return self.final_func(self.state)
        else:
            #print(self.state)
            return self.state
