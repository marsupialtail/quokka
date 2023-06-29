# Extreme Feature Engineering Pipelines

We who have to predict things to make money in the markets oftentimes do so based on floating point numbers. A lot of floating point numbers. It is not uncommon to have up to thousands or tens of thousands of features for a target variable. 

Typical distributed data processing systems like SQL query engines or Spark are not designed to deal with these extremely wide tables. Let's consider this simple data processing pipeline written in Quokka:

```
qc = QuokkaContext()
# data has 10k floating point columns
data = qc.read_parquet("s3://1M-10k-floats/*")
z = data.approximate_quantile(data.schema, [0.1, 0.9]).collect()
print(z)
clipped = data.clip(z.to_dict())
print(clipped.covariance())
```

It should be rather self-evident what we are trying to do -- compute the covariance of winsorized features. This is an important step in examining the relationship between our features for any kind of statistical analysis or machine learning down the line. 

If the data is small and fits on one machine, Polars can easily handle this task. If the data is large but don't have too many columns, we can use SparkSQL with its built in `approxQuantile` function. However, in the dataset we care about, we happen to have 10k floating point columns, and one million rows. SparkSQL's `approxQuantile` method is not built for this scale and never finishes even with a massive cluster.

## How Quokka implements approxQuantile

Quokka's approximate quantile method is built to handle this kind of use cases. In Python? No, sorry, we took the easy way out and built a C binding in one day, starting from the conveniently Apache 2.0 licensed C++ code for t-digest in Apache Arrow. Since Quokka's internal data representation is Arrow, it is very easy to extend it with custom C++ plugins, some examples of which are shown in this [repo](https://github.com/marsupialtail/ldb/blob/master/src/tdigest.cc).

Quokka not only supports custom C++ plugins for stateless operations, but also *stateful* operations. It uses Pybind to let the C++ handle state while invoking handler methods at the Python level. This is almost exactly how Pytorch C++ extensions work.

We are currently making the extension process more pleasant and better documented, so please stay tuned. 

## Result

As a result, the winsorization pipeline completes in **about 1 minute** on our 1M by 10k dataset with four r6id.2xlarge workers on AWS, while Spark's approxQuantile method always crashes. 

We are constantly writing new extensions for key workloads we find useful in practice, such as asof and range joins, complex event processing and bloom filters.

If you have a workload in need of a custom operator, feel free to get in touch.
