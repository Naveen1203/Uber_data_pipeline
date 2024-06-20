from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# Initialize Spark session with the appropriate Kafka package
spark = SparkSession.builder \
    .appName("localtotopic") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

# Define schema for the data
schema = StructType([
    StructField("null", IntegerType(), True),
    StructField("key", TimestampType(), True),
    StructField("fare_amount", FloatType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("passenger_count", IntegerType(), True)
])

# Read data from CSV file
# Assuming the script and the CSV file are in the same directory
current_dir = os.path.dirname(os.path.realpath(__file__))
csv_file_path = os.path.join(current_dir, "uber.csv")

# Use os.path.normpath to create a valid path string
normalized_csv_file_path = os.path.normpath(csv_file_path)

df1 = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load(f"file:///{normalized_csv_file_path}")

# Prepare data to be written to Kafka
select_df = df1.select("fare_amount", "pickup_datetime", "pickup_longitude", "pickup_latitude", "dropoff_longitude","dropoff_latitude","passenger_count")
kafka_df = select_df.selectExpr("CAST(fare_amount AS STRING) AS key", "to_json(struct(*)) AS value")

# Write data to Kafka topic
kafka_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "uber_topic") \
    .save()

print("Data successfully loaded into Kafka topic 'first_topic'.")

# Stop Spark session
spark.stop()
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 localtotopic.py