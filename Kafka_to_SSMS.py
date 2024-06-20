from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

current_dir = os.path.dirname(os.path.realpath(__file__))
sqlserver_jdbc_jar = os.path.join(current_dir, "mssql-jdbc-12.6.2.jre11.jar")

# Initialize Spark session with required configurations
spark = SparkSession.builder \
    .appName("test") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.jars", sqlserver_jdbc_jar) \
    .getOrCreate()

print("Starting Kafka read operation...")

# Read data from Kafka topic
df1 = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("startingOffsets", "earliest") \
    .option("subscribe", "uber_topic") \
    .load()

# Print the schema of the DataFrame
print("Schema of the Kafka DataFrame:")
df1.printSchema()

# Count the number of records read from Kafka
record_count = df1.count()
print("Number of records read from Kafka:", record_count)

# Select key and value as strings
kafka_df = df1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Define the schema for the JSON data (adjust the field names and types as needed)
schema = StructType([
    StructField("fare_amount", FloatType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("passenger_count", IntegerType(), True)
])

# Parse the JSON values
parsed_df = kafka_df.withColumn("value", from_json(col("value"), schema))

# Select the parsed fields along with the key
final_df = parsed_df.select("key", "value.*")

# Apply filters
final_df = final_df.filter((col("passenger_count") <= 6) &
                           (col("pickup_longitude") != 0) &
                           (col("pickup_latitude") != 0) &
                           (col("dropoff_longitude") != 0) &
                           (col("dropoff_latitude") != 0))

# Add additional columns
df3 = final_df.withColumn("month", month(col("pickup_datetime"))) \
              .withColumn("year", year(col("pickup_datetime"))) \
              .withColumn("date", dayofmonth(col("pickup_datetime"))) \
              .withColumn("location_id", lit(None).cast(IntegerType())) \
              .withColumn("fare_id", monotonically_increasing_id() + 1) \
              .withColumn("pickup_id", col("fare_id").cast(IntegerType())) \
              .withColumn("location_id", col("fare_id").cast(IntegerType()))

# Select necessary dataframes
df_fare = df3.select("fare_id", "pickup_id", "location_id", "passenger_count", "fare_amount")
df_location = df3.select("location_id", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude")
df_pickup = df3.select("pickup_id", "month", "year", "date")

# Show dataframes
df_fare.show(truncate=False)
df_location.show(truncate=False)
df_pickup.show(truncate=False)

# Define JDBC URL for SQL Server
jdbc_url = "jdbc:sqlserver://DESKTOP-L2SUA88\\SQLEXPRESS:1433;databaseName=Uber;encrypt=true;trustServerCertificate=true"
table_name_fare = "dbo.Fare"
table_name_location = "dbo.Location"
table_name_pickup = "dbo.Pickup"

# Configure connection properties
connection_properties = {
    "user": "sa",
    "password": "*********",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Write DataFrames to SQL Server tables using JDBC
df_fare.write.jdbc(url=jdbc_url, table=table_name_fare, mode="overwrite", properties=connection_properties)
df_location.write.jdbc(url=jdbc_url, table=table_name_location, mode="overwrite", properties=connection_properties)
df_pickup.write.jdbc(url=jdbc_url, table=table_name_pickup, mode="overwrite", properties=connection_properties)

# Stop SparkSession
spark.stop()
