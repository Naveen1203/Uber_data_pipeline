from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("test").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1").getOrCreate()
print("Starting Kafka read operation...")

#df1 = spark.read.format("csv").option("header", "true").load()
df1 = spark.read.format("Kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","first_topic").option("startingOffsets","earliest").option("header", "true").load()
print("Schema of the Kafka DataFrame:*********************************************************************************************************************************************")
df1.printSchema()
print("Number of records read from Kafka:", df1.count())
kafka_df = df1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

kafka_df.show(truncate=False)
print("****************************************************************************",kafka_df.count())
spark.stop()
#df2 = kafka_df.writeStream.outputMode("append").format("memory").queryName("taxi").start()
#df2.awaitTermination()
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 transformation.py