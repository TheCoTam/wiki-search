from pyspark.sql import Row
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("SaveToHBase") \
    .config("spark.hbase.host", "localhost") \
    .config("spark.hbase.port", "2181") \
    .getOrCreate()

# HBase Configuration

# Chuẩn bị DataFrame
data = [
    Row(Id="1", Name="Alice", Age=25),
    Row(Id="2", Name="Bob", Age=30)
]
df = spark.createDataFrame(data)

# Lưu vào HBase
df.write \
    .format("org.apache.hadoop.hbase.spark") \
    .option("hbase.zookeeper.quorum", "localhost") \
    .option("hbase.zookeeper.property.clientPort", "2181") \
    .option("hbase.mapred.outputtable", "test_table") \
    .option("key.column", "Id") \
    .option("column.family", "info") \
    .save()
