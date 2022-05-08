# Creds to Tim Tully

# package requirements: "torch", "torchvision", "img2vec_pytorch", "opencv-utils", "opencv-python"

import boto3
import numpy as np
import os
import cv2
import os
from img2vec_pytorch import Img2Vec
from PIL import Image
from pyquokka.sql import Executor
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.dataset import InputFilesDataset

# in Quokka, downstream operators always receive a list of outputs from the upstream. So if the upstream produces some arbitrary type A, downstream will receive a list of A.
# Type preservation happens if all operators take a list of numpys say, and return a single numpy. Then all the operators in the graph will operate on similar data types
# Types can become more complicated if oeprators start producing nested types etc. etc.

ips = ['localhost']
workers = 1

class StageOne(Executor):
    def __init__(self, model_bucket, model_key, config_bucket, config_key) -> None:
        self.model_file = (model_bucket, model_key)
        self.config_file = (config_bucket, config_key)
        self.net = None
    def execute(self,batches,stream_id, executor_id):
        # the batches are going to be a list of (filename, images in numpy array format, gotten from cv2.imread)
        if self.net is None:
            s3_client = boto3.client("s3")
            s3_client.download_file(self.model_file[0],self.model_file[1],"/data/model.caffemodel")
            s3_client.download_file(self.config_file[0],self.config_file[1],"/data/deploy.txt")
            self.net = cv2.dnn.readNetFromCaffe("/data/model.caffemodel","/data/deploy.txt")
            os.remove("/data/model.caffemodel")
            os.remove("/data/deploy.txt")
        
        ret_vals = {}
        for thing in batches:
            filename, obj = thing
            img = cv2.imdecode(np.asarray(bytearray(obj)), cv2.IMREAD_COLOR)
            ret_vals[filename] = []
            h, w = img.shape[:2]
            blob = cv2.dnn.blobFromImage(cv2.resize(img, (300, 300)), 1.0,(300, 300), (104.0, 117.0, 123.0))
            self.net.setInput(blob)
            faces = self.net.forward()
            
            for i in range(faces.shape[2]):
                confidence = faces[0, 0, i, 2]
                if confidence > 0.5:
                    box = faces[0, 0, i, 3:7] * np.array([w, h, w, h])
                    (x, y, x1, y1) = box.astype("int")
                    roi_color = img[y:y1, x:x1]
                    ret_vals[filename].append(roi_color)
        
        # returns a dict of filename -> list of images, which will be pickled and pushed as a unit to a downstream node.
        return ret_vals
        
        # in the future probably batch the inputs, but for now we don't have to.
    def done(self,executor_id):
        del self.net

class StageTwo(Executor):
    def __init__(self) -> None:
        self.img2vec = None
    def execute(self, batches, stream_id, executor_id):
        if self.img2vec is None:
            self.img2vec = Img2Vec(cuda=False, model='resnet-18')
        
        # each object will be a dict of filename -> list of images
        for obj in batches:
            for filename in obj:
                images = obj[filename]
                for image in images:
                    img = Image.fromarray(image)
                    vec = self.img2vec.get_vec(img, tensor=True)
                    print(vec.numpy().flatten())

print("WARNING DO NOT PUSH THIS TO GITHUB!")

def partition_key1(data, source_channel, target_channel):

    if source_channel//8 == target_channel:
        return data
    else:
        return None

def pass_thru(data, source_channel, target_channel):

    if source_channel == target_channel:
        return data
    else:
        return None

task_graph = TaskGraph()

reader = InputFilesDataset("fddb")
images = task_graph.new_input_reader_node(reader, {ip:8 for ip in ips[:workers]})
stage_one = StageOne("quokka-examples","res10_300x300_ssd_iter_140000.caffemodel","quokka-examples","deploy.prototxt.txt")
faces = task_graph.new_non_blocking_node({0:images}, None, stage_one, {ip:1 for ip in ips[:workers]}, {0:partition_key1})
stage_two = StageTwo()
vecs = task_graph.new_non_blocking_node({0:faces}, None, stage_two, {ip:1 for ip in ips[:workers]}, {0:pass_thru})

import time
task_graph.create()
start = time.time()
task_graph.run_with_fault_tolerance()
print(time.time() - start)
#print(ray.get(result.to_pandas.remote()))
