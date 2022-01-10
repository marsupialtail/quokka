import findspark
try:
    spark.stop()
except:
    pass

findspark.init(spark_home='/home/ziheng/anaconda3/envs/neurodb/lib/python3.7/site-packages/pyspark')
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession



conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.1')

scxt = SparkContext(conf=conf)

hadoopConf = scxt._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.DefaultAWSCredentialsProviderChain')
hadoopConf.set('fs.s3a.endpoint', 's3-us-west-2.amazonaws.com')
hadoopConf.set('com.amazonaws.services.s3a.enableV4', 'true')
hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

 
spark = SparkSession(scxt)
df_a = spark.read.csv('s3a://yugan/a.csv', header=True)
df_b = spark.read.csv('s3a://yugan/b.csv', header=True)
print(df_a.columns)
print(df_b.columns)
print(df_a.show())
