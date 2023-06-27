import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import json
import pyarrow.json as pajson
from io import BytesIO
import boto3
import os
import redis
from collections import deque
import polars
import gc
from pyarrow.fs import S3FileSystem, LocalFileSystem
from pyarrow.dataset import FileSystemDataset, ParquetFileFormat
from pyquokka.sql_utils import filters_to_expression
import multiprocessing
import concurrent.futures
import time
import warnings
import random
import ray
import math
import asyncio
import numpy as np

def overlap(a, b):
    return max(-1, min(a[1], b[1]) - max(a[0], b[0]))