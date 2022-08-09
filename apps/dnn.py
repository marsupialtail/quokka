# Creds to Tim Tully

# package requirements: "torch", "torchvision", "img2vec_pytorch", "opencv-utils", "opencv-python"

import boto3
import numpy as np
from pyquokka.executors import Executor
from pyquokka.quokka_runtime import TaskGraph
from pyquokka.dataset import InputS3FilesDataset
from pyquokka.utils import QuokkaClusterManager, LocalCluster
from transformers import AutoTokenizer
import torch.nn.functional as F
import torch.nn as nn
import torch
import time
import os

# in Quokka, downstream operators always receive a list of outputs from the upstream. So if the upstream produces some arbitrary type A, downstream will receive a list of A.
# Type preservation happens if all operators take a list of numpys say, and return a single numpy. Then all the operators in the graph will operate on similar data types
# Types can become more complicated if oeprators start producing nested types etc. etc.

# manager = QuokkaClusterManager()
# cluster = manager.get_cluster_from_json("config.json")
cluster = LocalCluster()

class OPTLearnedPositionalEmbedding(nn.Embedding):
    """
    This module learns positional embeddings up to a fixed maximum size.
    """

    def __init__(self, num_embeddings: int, embedding_dim: int):
        # OPT is set up so that if padding_idx is specified then offset the embedding ids by 2
        # and adjust num_embeddings appropriately. Other models don't have this hack
        self.offset = 2
        super().__init__(num_embeddings + self.offset, embedding_dim)

    def forward(self, attention_mask: torch.LongTensor, past_key_values_length: int = 0):
        """`input_ids_shape` is expected to be [bsz x seqlen]."""
        attention_mask = attention_mask.long()

        # create positions depending on attention_mask
        positions = (torch.cumsum(attention_mask, dim=1).type_as(attention_mask) * attention_mask).long() - 1

        # cut positions if `past_key_values_length` is > 0
        positions = positions[:, past_key_values_length:]

        return super().forward(positions + self.offset)

class TokenizeandEmbed(Executor):
    def __init__(self, model_bucket) -> None:
        self.model_bucket = model_bucket
        self.token_embed = None
        self.pos_embed = None
        self.tokenizer = None
        self.curr_pos = {}
    
    def execute(self,batches,stream_id, executor_id):
        if self.token_embed is None:
            s3_client = boto3.client("s3")
            if not os.path.exists("/data/decoder.embed_positions.weight"):
                s3_client.download_file(self.model_bucket,"decoder.embed_positions.weight","/data/decoder.embed_positions.weight")
            self.pos_embed = torch.from_numpy(np.load("/data/decoder.embed_positions.weight")).float()
            self.lps = OPTLearnedPositionalEmbedding(2050, 768)
            self.lps.weight = nn.Parameter(self.pos_embed)
            if not os.path.exists("/data/decoder.embed_tokens.weight"):
                s3_client.download_file(self.model_bucket,"decoder.embed_tokens.weight","/data/decoder.embed_tokens.weight")
            self.token_embed = torch.from_numpy(np.load("/data/decoder.embed_tokens.weight")).float()
            self.tokenizer = AutoTokenizer.from_pretrained("facebook/opt-30b", use_fast=False)
            self.tokenizer.add_bos_token = False

        for batch in batches:
            print(batch)
            prompts, ids, ids_to_drop = batch

            for id in ids_to_drop:
                del self.curr_pos[id]

            input_ids = self.tokenizer(prompts, return_tensors="np").input_ids
            position_ids = []    
            offsets = [0]
            modes = []
            for input_id, seq_id in zip(input_ids, ids):
                # increment mode 
                if seq_id in self.curr_pos:
                    position_ids.append([self.curr_pos[seq_id]])
                    self.curr_pos[seq_id] += 1
                    offsets.append(offsets[-1] + 1)
                    modes.append("I")
                # prompt mode
                else:
                    position_ids.append(list(range(len(input_id))))
                    self.curr_pos[seq_id] = len(input_id)
                    offsets.append(offsets[-1] + len(input_id))
                    modes.append("P")
            input_ids = torch.LongTensor([i for j in input_ids for i in j])
            position_ids = torch.LongTensor([i for j in position_ids for i in j])
            # the shape is not B * d
            print(input_ids, position_ids)
            outputs = F.embedding(input_ids, self.token_embed)  + self.lps.forward(torch.LongTensor([1] * len(input_ids)).unsqueeze(0)).squeeze()
            yield (outputs, offsets, modes ,ids, ids_to_drop)

    def done(self,executor_id):
        return

class Intermediate(Executor):
    def __init__(self, model_bucket, layer_start, layer_end, k, h) -> None:
        self.model_bucket = model_bucket
        self.weights = {}
        
        self.layer_start = layer_start
        self.layer_end = layer_end
        self.layers = list(range(self.layer_start, self.layer_end))
        self.kv_state = {i:{} for i in self.layers}

        self.h = h
        self.k = k
    
    # naive implementation of Orca.
    def execute(self,batches,stream_id, executor_id):
        
        # let's assume that each batch is an actual batch of embedding - ID pairs. 
        # or we could assume each batch is an embedding - ID pair, but I don't think it quite make sense
        # for a batch not to pass through the net altogether. We can probably study that later.

        torch.set_num_threads(8)

        if len(self.weights) == 0:
            s3_client = boto3.client("s3")
            for layer in self.layers:
                names = ["decoder.layers." + str(layer) + ".self_attn.q_proj.weight"
                ,"decoder.layers." + str(layer) + ".self_attn.k_proj.weight"
                ,"decoder.layers." + str(layer) + ".self_attn.v_proj.weight"
                ,"decoder.layers." + str(layer) + ".self_attn.q_proj.bias"
                ,"decoder.layers." + str(layer) + ".self_attn.k_proj.bias"
                ,"decoder.layers." + str(layer) + ".self_attn.v_proj.bias"
                ,"decoder.layers." + str(layer) + ".self_attn.out_proj.weight"
                ,"decoder.layers." + str(layer) + ".self_attn.out_proj.bias"
                ,"decoder.layers." + str(layer) + ".self_attn_layer_norm.weight"
                ,"decoder.layers." + str(layer) + ".self_attn_layer_norm.bias"
                ,"decoder.layers." + str(layer) + ".fc1.weight"
                ,"decoder.layers." + str(layer) + ".fc2.weight"
                ,"decoder.layers." + str(layer) + ".fc1.bias"
                ,"decoder.layers." + str(layer) + ".fc2.bias"
                ,"decoder.layers." + str(layer) + ".final_layer_norm.weight"
                ,"decoder.layers." + str(layer) + ".final_layer_norm.bias"]

                for name in names:
                    if not os.path.exists("/data/" + name):
                        s3_client.download_file(self.model_bucket,name,"/data/" + name)
                    self.weights[name] = torch.from_numpy(np.load("/data/" + name)).float()
        
        for batch in batches:
            # each batch will compose of four items. 
            # the first is a matrix of shape B * d, it could be composed of b requests.
            # the second is a list of length b +1 where element i tells you the index row at which request i starts and element i + 1 tells you where it ends.
            # the third is a list of length b where element i tells you if that request is in increment or prompt mode.
            # the fourth is a list of length b where element i tells you the id of the element
            # currently only support passing in prompt all at once. So scheduler must guarantee this.

            inputs, offsets, modes, ids, ids_to_drop = batch            
            
            b = len(ids)
            for layer in self.layers:
                print("===========doing layer ", layer, "===================")
                for id in ids_to_drop:
                    # might want to make sure memory is actually freed here
                    del self.kv_state[layer][id]

                # assume that the QKV matrix is d * (h * k) format.
                q_proj_weight = self.weights["decoder.layers." + str(layer) + ".self_attn.q_proj.weight"]
                k_proj_weight = self.weights["decoder.layers." + str(layer) + ".self_attn.k_proj.weight"]
                v_proj_weight = self.weights["decoder.layers." + str(layer) + ".self_attn.v_proj.weight"]
                q_proj_bias = self.weights["decoder.layers." + str(layer) + ".self_attn.q_proj.bias"]
                k_proj_bias = self.weights["decoder.layers." + str(layer) + ".self_attn.k_proj.bias"]
                v_proj_bias = self.weights["decoder.layers." + str(layer) + ".self_attn.v_proj.bias"]


                out_proj_weight = self.weights["decoder.layers." + str(layer) + ".self_attn.out_proj.weight"]
                out_proj_bias = self.weights["decoder.layers." + str(layer) + ".self_attn.out_proj.bias"]
                attn_layer_norm_weights = self.weights["decoder.layers." + str(layer) + ".self_attn_layer_norm.weight"]
                attn_layer_norm_bias = self.weights["decoder.layers." + str(layer) + ".self_attn_layer_norm.bias"]

                fc1_weight = self.weights["decoder.layers." + str(layer) + ".fc1.weight"]
                fc2_weight = self.weights["decoder.layers." + str(layer) + ".fc2.weight"]
                fc1_bias = self.weights["decoder.layers." + str(layer) + ".fc1.bias"]
                fc2_bias = self.weights["decoder.layers." + str(layer) + ".fc2.bias"]

                fc_layer_norm_weights = self.weights["decoder.layers." + str(layer) + ".final_layer_norm.weight"]
                fc_layer_norm_bias = self.weights["decoder.layers." + str(layer) + ".final_layer_norm.bias"]

                # these things are B * (h * k)
                residual = inputs
                inputs = F.layer_norm(inputs, (self.h * self.k,) , weight= attn_layer_norm_weights, bias=attn_layer_norm_bias)

                qvk_combined = torch.cat([q_proj_weight, v_proj_weight, k_proj_weight],axis=0).reshape(3,-1,self.h * self.k).permute(2,1,0).reshape(self.h*self.k,-1)
                qvk_combined_b = torch.cat([q_proj_bias, v_proj_bias, k_proj_bias], axis=0).reshape(3,self.h* self.k).transpose(1,0).reshape(-1,)
                
                qvk_combined_states = torch.matmul(inputs, qvk_combined) + qvk_combined_b
                qvk_combined_states = qvk_combined_states.reshape(inputs.shape[0], -1, 3)
                q, v, k = torch.split(qvk_combined_states,1, dim=2)
                q = q.reshape(-1,self.h * self.k)
                v = v.reshape(-1,self.h * self.k)
                k = k.reshape(-1,self.h * self.k)

                # now split as in Orca. We don't got the fancy ragged attention kernel.
                values_list = []
                for element in range(b):
                    start = offsets[element]
                    end = offsets[element + 1]
                    mode = modes[element]
                    id = ids[element]
                    length = end - start
                    assert not (mode == "I" and length > 1)
                    # prompt must be supplied all at once.
                    if mode == "P":
                        assert id not in self.kv_state[layer]

                    q_slice = q[start:end]
                    k_slice = k[start:end]
                    v_slice = v[start:end]

                    if mode == "P":
                        self.kv_state[layer][id] = (k_slice, v_slice)
                        mask = torch.triu(torch.ones(length, length) * -3.4e38, diagonal=1)

                        # the slices have shape l * (h * k)
                        # make this h * l * k
                        q_slice = q_slice.view(length, self.h, self.k).permute(1,0,2) / np.sqrt(self.k)
                        #print(q_slice)
                        # make this h * k * l
                        k_slice = k_slice.view(length, self.h, self.k).permute(1,2,0)
                        v_slice = v_slice.view(length, self.h, self.k).permute(1,0,2)
                        # this will be h * l * l. mask should be broadcast
                        logits = torch.baddbmm(mask, q_slice, k_slice)
                        scaled_logits = F.softmax(logits, -1)
                        # this will be h * l * k, then transpose to l * h * k
                        
                        values = torch.bmm(scaled_logits, v_slice).permute(1,0,2).reshape(length, self.h * self.k)

                        #values = valuesp[0].squeeze(0)

                    elif mode == "I":
                        #the slices have shape 1 * (h * k)
                        assert id in self.kv_state[layer]
                        # length should be 1
                        q_slice = q_slice.view(self.h, 1, self.k)
                        cached_k, cached_v = self.kv_state[layer][id]
                        new_k = torch.cat(cached_k, k_slice).contiguous()
                        new_v = torch.cat(cached_v, v_slice).contiguous()

                        # you might want to make sure that the old memory is actually freed
                        self.kv_state[layer][id] = (new_k, new_v)
                        k_slice = new_k.view(-1, self.h, self.k).permute(1,2,0)
                        v_slice = new_v.view(-1, self.h, self.k).permute(1,0,2)
                        # output should be h * 1 * l
                        logits = torch.bmm(q_slice, k_slice)
                        scaled_logits = F.softmax(logits, - 1)
                        # this will be h * 1 * k. bmm between h * 1 * l and h * l * k, really batched gemv
                        values = torch.bmm(scaled_logits, v_slice).view(1, self.h * self.k)
                        
                    else:
                        raise Exception
                    
                    values_list.append(values)
                
                # values now will have shape B * d
                values = torch.cat(values_list)
                result = F.linear(values, out_proj_weight, bias= out_proj_bias)
                result += residual
                # at this point, result should have shape l * d
                residual = result
                result = F.layer_norm(result, (self.h * self.k,) , weight=fc_layer_norm_weights, bias = fc_layer_norm_bias)
                result = F.linear(result, fc1_weight, fc1_bias)
                result = F.relu(result)
                result = F.linear(result, fc2_weight, fc2_bias)
                inputs = result + residual
                print(inputs)
            
            output_batch = (inputs, offsets, modes, ids)
            
            yield output_batch

        
    def done(self,executor_id):
        del self.weights
        del self.kv_state

class OutputGenerator(Executor):
    def __init__(self, model_bucket, h, k) -> None:
        self.tokenizer = None
        self.model_bucket = model_bucket
        self.h = h
        self.k = k
        self.layer_norm_weight = None
        self.layer_norm_bias = None
        self.token_embed = None
    
    def execute(self,batches,stream_id, executor_id):
        if self.tokenizer is None:
            self.tokenizer = AutoTokenizer.from_pretrained("facebook/opt-30b", use_fast=False)
            self.tokenizer.add_bos_token = False
            
            s3_client = boto3.client("s3")
            if not os.path.exists("/data/decoder.layer_norm.weight"):
                s3_client.download_file(self.model_bucket,"decoder.layer_norm.weight","/data/decoder.layer_norm.weight")
            self.layer_norm_weight = torch.from_numpy(np.load("/data/decoder.layer_norm.weight")).float()
            if not os.path.exists("/data/decoder.layer_norm.bias"):
                s3_client.download_file(self.model_bucket,"decoder.layer_norm.bias","/data/decoder.layer_norm.bias")
            if not os.path.exists("/data/decoder.embed_tokens.weight"):
                s3_client.download_file(self.model_bucket,"decoder.embed_tokens.weight","/data/decoder.embed_tokens.weight")
            self.token_embed = torch.from_numpy(np.load("/data/decoder.embed_tokens.weight")).float().t()
            self.layer_norm_bias = torch.from_numpy(np.load("/data/decoder.layer_norm.bias")).float()
            self.tokenizer = AutoTokenizer.from_pretrained("facebook/opt-30b", use_fast=False)
            self.tokenizer.add_bos_token = False

        for batch in batches:
            inputs, offsets, modes, ids = batch
            inputs = F.layer_norm(inputs, [self.h * self.k], weight=self.layer_norm_weight, bias = self.layer_norm_bias)
            print("done", time.time())
            print(inputs, offsets, modes, ids)
            logits = torch.matmul(inputs, self.token_embed)
            print("logits shape", logits.shape)
            next_token = torch.argmax(logits, -1).tolist()
            next_gen_token_str = self.tokenizer.decode(next_token, clean_up_tokenization_spaces=True)
            print(next_gen_token_str)

        return None

    def done(self, executor_id):
        return None

# this dataset will generate a sequence of numbers, from 0 to limit. Channel 
class FakeInputBatchGenerator:
    def __init__(self, delay, total= 2) -> None:
        
        self.delay = delay
        self.total = total
        self.num_channels = None

        self.prompts = [["Hey what's up!"], ["How are you","Are you at home"]]
        self.ids = [[0],[2,3]]

    def set_num_channels(self, num_channels):
        self.num_channels = num_channels

    def get_next_batch(self, channel, pos=None):
        # let's ignore the keyword pos = None, which is only relevant for fault tolerance capabilities.
        assert self.num_channels is not None
        
        # for batch_no in range(self.total):
        #     time.sleep(self.delay)
        #     # generate a batch
        #     curr_batch = ...
        for i in range(self.total):
            print("Batch", i , "start", time.time())
            yield None, (self.prompts[i], self.ids[i], set())
            time.sleep(20)
            

task_graph = TaskGraph(cluster)

generator = FakeInputBatchGenerator(0.1, total=2)
requests = task_graph.new_input_reader_node(generator,ip_to_num_channel={'localhost': 1}, )
stage_one = TokenizeandEmbed("opt-weights-175")
embeddings = task_graph.new_non_blocking_node({0:requests}, stage_one)
stage_two = Intermediate("opt-weights-175",0,12,64,12)
intermediate = task_graph.new_non_blocking_node({0:embeddings}, stage_two)
# stage_three = Intermediate("opt-weights-175",6,12,64,12)
# intermediate = task_graph.new_non_blocking_node({0:embeddings}, stage_three)
output = OutputGenerator("opt-weights-175", 12, 64)
outputs = task_graph.new_non_blocking_node({0:intermediate}, output)

task_graph.create()
start = time.time()
task_graph.run()
print(time.time() - start)