from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("Pyspark Test")\
        .getOrCreate()


data = [("Alice", 30), ("James", 31)]
df = spark.createDataFrame(data, ["Name", "Age"])

df.show

spark.stop()

