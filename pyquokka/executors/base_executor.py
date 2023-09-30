import os
# os.environ["POLARS_MAX_THREADS"] = "1" 
import polars
import pandas as pd
os.environ['ARROW_DEFAULT_MEMORY_POOL'] = 'system'
import redis
import pyarrow as pa
import time
import os, psutil
import pyarrow.parquet as pq
import pyarrow.csv as csv
from collections import deque
import pyarrow.compute as compute
import random
import sys
from pyarrow.fs import S3FileSystem, LocalFileSystem
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import concurrent.futures
import duckdb
import numpy as np
import multiprocessing
from pyquokka.windowtypes import *
from threadpoolctl import threadpool_limits

class Executor:
    def __init__(self) -> None:
        raise NotImplementedError
    def execute(self,batches,stream_id, executor_id):
        raise NotImplementedError
    def done(self,executor_id):
        raise NotImplementedError