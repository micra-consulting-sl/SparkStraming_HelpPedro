
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.2-bin-hadoop3.2"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages io.delta:delta-core_2.12:0.8.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog pyspark-shell'

import findspark
findspark.init()


from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# create the session
spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.ui.port", "4050") \
        .getOrCreate()

spark.version


from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "topic1") \
  .load()
  
schema = StructType(
    [
        StructField('id', StringType(), True),
        StructField('name', StringType(), True),
        StructField('last_name', StringType(), True),
        StructField('position', StringType(), True),
        StructField('transport', StringType(), True),
        # StructField('text', StringType(), True)
    ]
)
df.printSchema()

dataset = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp") \
    .withColumn("value", from_json("value", schema)) \
    .select(col('key'), col("timestamp"), col('value.*'))