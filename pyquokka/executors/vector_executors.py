from pyquokka.executors.base_executor import * 

class DFProbeDataStreamNNExecutor1(Executor):

    """
    This executor is optimized for the scenario where we have a small set of probes against a large set of vectors.
    This broadcasts the probes on different machines, where each read only a subset of the vectors.
    A global reduction stage is needed at the end.
    """

    def __init__(self, vec_col, query_df, query_vec_col, k) -> None:

        assert type(query_df) == polars.DataFrame
        self.vec_col = vec_col
        query_vecs = np.stack(query_df[query_vec_col].to_numpy())
        self.query_df = query_df
        self.query_vecs = query_vecs / np.linalg.norm(query_vecs, axis = 1, keepdims = True)
        self.k = k

    def execute(self, batches, stream_id, executor_id):
        
        start = time.time()
        batch = pa.concat_tables(batches)
        print("starting stage 0, with shape", len(batch))
        vectors = np.stack(batch[self.vec_col].to_numpy())
        normalized_vectors = vectors / np.linalg.norm(vectors, axis = 1, keepdims = True)

        with threadpool_limits(limits=8, user_api='blas'):
            distances = np.dot(normalized_vectors, self.query_vecs.T)
        indices = np.argsort(distances, axis = 0)[-self.k:].T.flatten()

        # import torch
        # torch.set_num_threads(8)
        # B = len(batch)
        # all_indices = []

        # for i in range(0, len(batch), B):
        #     vectors = torch.from_numpy(np.stack(batch[self.vec_col].to_numpy()[i: i + B])).cuda().half()
        #     normalized_vectors = vectors / vectors.norm(dim=1).unsqueeze(1)
        #     query_vecs = torch.from_numpy(self.query_vecs).cuda().half().T
        #     distances = torch.matmul(normalized_vectors, query_vecs)
        #     indices = torch.topk(distances, self.k, dim = 0).indices.T.flatten().cpu().numpy()

        #     all_indices.append(indices)
            # you could be smarter here and keep track of separate candidate sets for each probe, but let's leave this for an intern.
        # return batch.take(np.concatenate(all_indices))
        
        print("stage 0 took", time.time() - start)
        return batch.take(np.concatenate(indices))
    
    def done(self, executor_id):
        return


class DFProbeDataStreamNNExecutor2(Executor):

    def __init__(self, vec_col, query_df, query_vec_col, k) -> None:

        assert type(query_df) == polars.DataFrame
        self.vec_col = vec_col
        query_vecs = np.stack(query_df[query_vec_col].to_numpy())
        self.query_df = query_df
        self.query_vecs = query_vecs / np.linalg.norm(query_vecs, axis = 1, keepdims = True)
        self.k = k
        self.state = None

    def execute(self, batches, stream_id, executor_id):

        # each batch should have shape len(query_df) * k
        # you could start throwing away stuff as each batch comes in, but you can just wait until the end if the number of files is reasonably small

        batch = pa.concat_tables(batches)
        if self.state is None:
            self.state = batch
        else:
            self.state = pa.concat_tables([self.state, batch])

    def done(self, executor_id):

        vectors = np.stack(self.state[self.vec_col].to_numpy())
        normalized_vectors = vectors / np.linalg.norm(vectors, axis = 1, keepdims = True)

        # the length of normalized_vectors is N * len(probe_df) * k, where N is the number of Parquet files.
        # each probe vector should now probe only its corersponding vectors
        # this is going to be some complicated math, but I used to be good at this stuff, so let's see how it goes.

        vector_dim = normalized_vectors.shape[-1]
        normalized_vectors = normalized_vectors.reshape(-1, len(self.query_df), self.k, vector_dim)
        N = normalized_vectors.shape[0]
        normalized_vectors = normalized_vectors.transpose(1, 0, 2, 3).reshape(len(self.query_df), N * self.k, vector_dim)

        # now normalized_vectors has shape len(probe_df) * (N * k) * dim, query_vecs has shape len(probe_df) * dim
        # we can now do the batch matrix multiplication
        query_vecs = self.query_vecs[:, np.newaxis, :]
        with threadpool_limits(limits=8, user_api='blas'):
            distances = np.matmul(normalized_vectors, query_vecs.transpose(0, 2, 1)).squeeze()
    
        # distances has shape len(probe_df) * (N * k)
        indices = np.argsort(distances, axis = 1)[:, -self.k:]
        # indices has shape len(probe_df) * k

        # print(indices)
        # print(self.state)

        results = []
        # each index has a value between 0 and N * k - 1, you need to translate that to an index into self.state
        for i in range(len(indices)):
            index_group = indices[i] // self.k 
            index_in_group = indices[i] % self.k
            new_indices = index_group * len(self.query_df) * self.k + i * self.k + index_in_group
            results.append(self.state.take(new_indices))
        
        matched_vectors = pa.concat_tables(results)
        join_indices = np.repeat(np.arange(len(self.query_df)), self.k)
        matched_queries = self.query_df[join_indices].to_arrow()
    
        return pa.Table.from_arrays(matched_vectors.columns + matched_queries.columns, names=matched_vectors.column_names + matched_queries.column_names)