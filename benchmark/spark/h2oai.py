import time

left = spark.read.csv("s3://h2oai-benchmark/left.csv",header=True,inferSchema=True)
small = spark.read.csv("s3://h2oai-benchmark/small.csv",header=True,inferSchema=True)
medium = spark.read.csv("s3://h2oai-benchmark/medium.csv",header=True,inferSchema=True)

start = time.time(); left.join(small.withColumnRenamed("id4","id10"), on = "id1").write.parquet("s3://h2oai-benchmark/spark2.parquet/"); print(time.time() - start)
start = time.time(); left.join(medium.withColumnRenamed("id4","id10").withColumnRenamed("id1","id11").withColumnRenamed("id5","id12"), on = "id2").write.parquet("s3://h2oai-benchmark/spark3.parquet/"); print(time.time() - start)
