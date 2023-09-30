# Creds to Tim Tully

# package requirements: "torch", "torchvision", "img2vec_pytorch", "opencv-utils", "opencv-python"

import boto3
import numpy as np
import os
import cv2
import os
from img2vec_pytorch import Img2Vec
from PIL import Image
from pyquokka.df import * 
from pyquokka.utils import LocalCluster, QuokkaClusterManager
import torch

# in Quokka, downstream operators always receive a list of outputs from the upstream. So if the upstream produces some arbitrary type A, downstream will receive a list of A.
# Type preservation happens if all operators take a list of numpys say, and return a single numpy. Then all the operators in the graph will operate on similar data types
# Types can become more complicated if oeprators start producing nested types etc. etc.

manager = QuokkaClusterManager()
cluster = manager.get_cluster_from_json("config.json")

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
            self.net = cv2.dnn.readNetFromCaffe("/data/deploy.txt","/data/model.caffemodel")
            os.remove("/data/model.caffemodel")
            os.remove("/data/deploy.txt")
        
        ret_vals = {}
        for thing in batches:
            try:
                filename, obj = thing
            except:
                print(thing)
                raise Exception
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
        self.counter = 0
    def execute(self, batches, stream_id, executor_id):

        torch.set_num_threads(8)
        print("PROCESSED",self.counter)
        if self.img2vec is None:
            self.img2vec = Img2Vec(cuda=False, model='resnet-18')
        
        # each object will be a dict of filename -> list of images
        for obj in batches:
            for filename in obj:
                images = obj[filename]
                for image in images:
                    self.counter += 1
                    try:
                        img = Image.fromarray(image)
                        vec = self.img2vec.get_vec(img, tensor=True)
                    except:
                        pass
                    #print(vec.numpy().flatten())
    def done(self, executor_id):
        pass

qc = QuokkaContext(cluster)
files = qc.read_files("s3://fddb/*")
stage_one = StageOne("quokka-examples","res10_300x300_ssd_iter_140000.caffemodel","quokka-examples","deploy.prototxt.txt")
faces = files.stateful_transform(stage_one)
stage_two = StageTwo()
vecs = files.stateful_transform(stage_two)
vecs.collect()