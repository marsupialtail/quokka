import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType

def run_pyspark_join(data_source_a, data_source_b, output_uri):
    with SparkSession.builder.appName("Test").getOrCreate() as spark:
        timing = []
        max = 10
        for i in range(max):
            start = time.time()
            df_a = spark.read.option("header", "true").csv(data_source_a)
            df_b = spark.read.option("header", "true").csv(data_source_b)
            result = df_a.join(df_b, on="key", how='inner')
            end = time.time()
            timing.append(end - start)

        timing_df = spark.createDataFrame(timing, FloatType())
        timing_df.coalesce(1).write.option("header", "false").mode("overwrite").csv(output_uri)
        timing_df.repartition(1).write.option("header", "false").mode("overwrite").csv(output_uri)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source_a', help="The URI for you CSV restaurant data, like an S3 bucket location.")
    parser.add_argument(
        '--data_source_b', help="The URI for you CSV restaurant data, like an S3 bucket location.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    args = parser.parse_args()

    run_pyspark_join(args.data_source_a, args.data_source_b, args.output_uri)