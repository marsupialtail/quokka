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
        batch = pa.concat_tables(batches)
        vectors = np.stack(batch[self.vec_col].to_numpy())
        normalized_vectors = vectors / np.linalg.norm(vectors, axis = 1, keepdims = True)
        distances = np.dot(normalized_vectors, self.query_vecs.T)
        indices = np.argsort(distances, axis = 0)[-self.k:].flatten()

        # you could be smarter here and keep track of separate candidate sets for each probe, but let's leave this for an intern.
        return batch.take(indices)
    
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
        batch = pa.concat_tables(batches)
        if self.state is None:
            self.state = batch
        else:
            self.state = pa.concat_tables([self.state, batch])

    def done(self, executor_id):
        vectors = np.stack(self.state[self.vec_col].to_numpy())
        normalized_vectors = vectors / np.linalg.norm(vectors, axis = 1, keepdims = True)
        with threadpool_limits(limits=8, user_api='blas'):
            distances = np.dot(normalized_vectors, self.query_vecs.T)
        indices = np.argsort(distances, axis = 0)[-self.k:]
        # indices will have shape k * num_queries
        flat_indices = indices.T.flatten()
        matched_vectors = polars.from_arrow(self.state.take(flat_indices))
        join_indices = np.repeat(np.arange(len(self.query_df)), self.k)
        matched_queries = self.query_df[join_indices]
        
        # upstream should already make sure there is no overlapping column names here
        return polars.concat([matched_queries, matched_vectors], how = "horizontal")