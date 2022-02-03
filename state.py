import pandas as pd
import numpy as np
import os

# this is the stupidest implementation one could do.
class PersistentStateVariable:
    def __init__(self, max_mem = 1024 * 1024 * 1024) -> None:
        
        self.file_num = 0
        self.max_mem = max_mem

        # ok this requires careful thinking. We must not allow two actors on the same machine to have the same filepath.
        # the filepath is based on a random number, so chances of collision is very small. 
        # in addiiton, mkdir is supposed to be atomic for the OS, so this will work.

        while True:
            try:
                random_number =  int(np.random.random() * 1000000)
                filepath = "/tmp/" + str(random_number)
                os.mkdir(filepath)
                self.filepath = filepath
                break
            except FileExistsError:
                continue

        self.in_memory_state = []
        self.disk_state = []
        self.file_num = 0
    
    def get_current_mem(self):
        return sum([i.memory_usage().sum() for i in self.in_memory_state])
    
    def append(self, batch : pd.DataFrame):
        if self.get_current_mem() + batch.memory_usage().sum() < self.max_mem:
            self.in_memory_state.append(batch)
        else:
            path = self.file_path + "/temp-" + str(self.file_num) + ".parquet"
            batch.to_parquet(path)
            self.disk_state.append(path)
            self.file_num += 1
    
    def __iter__(self):

        for batch in self.in_memory_state:
            yield batch
        for file in self.disk_state:
            yield pd.read_parquet(file)
    
    def __len__(self):

        return len(self.in_memory_state) + len(self.disk_state)