import threading
from collections import deque
import pyarrow as pa
import pyarrow.parquet as pq
from threading import Lock
import os
import polars
import pandas as pd
import pickle


class DiskFile:
    def __init__(self,filename) -> None:
        self.filename = filename
    def delete(self):
        os.remove(self.filename)

class DiskQueue:
    def __init__(self, parents , prefix, disk_location) -> None:
        self.in_mem_portion =  {(parent, channel): deque() for parent in parents for channel in parents[parent]}
        self.prefix = prefix
        self.file_no = 0
        self.mem_limit = 1e9 # very small limit to test disk flushing. you want to set it to total_mem / channels / 2 or something.
        self.mem_usage = 0
        self.disk_location = disk_location

    # this is now blocking on disk write. 
    def append(self, key, table, format):
        size = table.nbytes
        if self.mem_usage +  size > self.mem_limit:
            # flush to disk
            filename = self.disk_location + "/" + self.prefix + str(self.file_no) + ".parquet"
            pq.write_table(table, filename)
            self.file_no += 1
            self.in_mem_portion[key].append((DiskFile(filename),format))
        else:
            self.mem_usage += size
            self.in_mem_portion[key].append((table,format))

    def retrieve_from_disk(self, key):
        pass

    def get_batches_for_key(self, key, num=None):
        def convert_to_format(table, format):
            if format == "polars":
                return polars.from_arrow(table)
            elif format == "pandas":
                return table.to_pandas()
            elif format == "custom":
                return pickle.loads(table.to_pandas()['object'][0])
            else:
                raise Exception("don't understand this format", format)
        batches = []
        # while len(self.in_mem_portion[key]) > 0 and type(self.in_mem_portion[key][0]) != tuple:
        #     batches.append(self.in_mem_portion[key].popleft())
        #     self.mem_usage -= batches[-1].nbytes
        # threading.Thread(target=self.retrieve_from_disk)
        end = len(self.in_mem_portion[key]) if num is None else max(num,len(self.in_mem_portion[key]) )
        for i in range(end):
            object, format = self.in_mem_portion[key][i]
            if type(object) == DiskFile:
                batches.append(convert_to_format(pq.read_table(object.filename), format))
                object.delete()
            else:
                batches.append(convert_to_format(object, format))
                self.mem_usage -= object.nbytes
        for i in range(end):
            self.in_mem_portion[key].popleft()
        return batches
    
    def keys(self):
        return self.in_mem_portion.keys()
    
    def len(self, key):
        return len(self.in_mem_portion[key])
