a = sc.textFile("s3://quokka-sorting/")
row = Row("text")
df = a.map(row).toDF()
df.sort("text").collect()

