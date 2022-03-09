import time

import findspark
try:
    spark.stop()
except:
    pass

findspark.init()
# spark_home='/home/ziheng/anaconda3/envs/neurodb/lib/python3.7/site-packages/pyspark'
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType


def get_spark_session():
    conf = SparkConf()
    conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.1')

    scxt = SparkContext(conf=conf)

    hadoopConf = scxt._jsc.hadoopConfiguration()
    hadoopConf.set('fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.DefaultAWSCredentialsProviderChain')
    hadoopConf.set('fs.s3a.endpoint', 's3-us-west-2.amazonaws.com')
    hadoopConf.set('com.amazonaws.services.s3a.enableV4', 'true')
    hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

    return SparkSession(scxt)


def run_pyspark_join(data_source_a, data_source_b, output_folder):
    spark = get_spark_session()

    start = time.time()
    df_a = spark.read.csv(data_source_a, header=True)
    df_b = spark.read.csv(data_source_b, header=True)
    output = df_a.join(df_b, on='key', how='inner')
    end = time.time()

    timing.append(end - start)

    timing_df = spark.createDataFrame(timing, FloatType())
    timing_df.coalesce(1).write.option("header", "false").mode("overwrite").csv(output_folder)
    timing_df.repartition(1).write.option("header", "false").mode("overwrite").csv(output_folder)
        

if __name__ == "__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument(
    #     '--data_source_a', help="The URI for you CSV restaurant data, like an S3 bucket location.")
    # parser.add_argument(
    #     '--data_source_b', help="The URI for you CSV restaurant data, like an S3 bucket location.")
    # parser.add_argument(
    #     '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    # args = parser.parse_args()
    data_source_a = 's3a://yugan/a-big.csv'
    data_source_b = 's3a://yugan/b-big.csv'
    output_folder = 'pyspark_join_timing'

    timing = []
    trials = 10
    for i in range(trials):
        t = run_pyspark_join(data_source_a, data_source_b, output_folder)
        timing.append(t)
    print(timing)

