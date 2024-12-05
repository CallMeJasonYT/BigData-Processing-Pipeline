import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 pyspark-shell'

# Define the schema for the JSON data
schema = StructType([
    StructField("name", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("time", StringType(), True),
    StructField("link", StringType(), True),
    StructField("position", StringType(), True),
    StructField("spacing", StringType(), True),
    StructField("speed", StringType(), True)
])

def main():
    spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .master("local[*]") \
        .getOrCreate()

    lines = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "172.27.73.63:9092")  \
        .option("subscribe", "vehicle_positions") \
        .load()

    # Assume `lines` is your initial DataFrame with the JSON data
    lines = lines \
    .selectExpr("CAST(value AS STRING) as json") 
    
    parsed_lines = lines \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

    aggr_lines = parsed_lines \
    .groupBy("time", "link") \
    .agg(count(col("name")).alias("vcount"), avg(col("speed")).alias("vspeed"))

    # Write the calculated DataFrame to console
    query = aggr_lines.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

    # Define a function to write processed data to MongoDB
    def write_processed_data_to_mongo(batch_df, batch_id):
        batch_df.write \
        .format("mongodb") \
        .option("spark.mongodb.connection.uri", "mongodb://127.0.0.1/project.vehicle_processed_data") \
        .mode("append") \
        .save()

    rawDataWriter = parsed_lines.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/pyspark/rawDataWriter_checkpoint") \
    .option("forceDeleteTempCheckpointLocation", "true") \
    .option("spark.mongodb.connection.uri", "mongodb://127.0.0.1/project.vehicle_raw_data") \
    .outputMode("append") \
    .start()

    processedDataWriter = aggr_lines.writeStream \
    .foreachBatch(write_processed_data_to_mongo) \
    .option("checkpointLocation", "/tmp/pyspark/processedDataWriter_checkpoint") \
    .outputMode("update") \
    .start()

    query.awaitTermination()
    rawDataWriter.awaitTermination()
    processedDataWriter.awaitTermination()

if __name__ == "__main__":
    main()
