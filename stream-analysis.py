#run using the following command
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 stream-analysis.py

 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json,to_timestamp,count,avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import lit
from pyspark.sql.functions import expr
from pyspark.sql.functions import window


   
spark = SparkSession.builder \
       .appName("stream-analysis") \
       .master("local[5]")\
       .config("spark.sql.shuffle.partitions", "10") \
       .getOrCreate()

spark.sparkContext.setLogLevel("WARN") #only show warn, not info logs


raw_stream = spark.readStream \
       .format("kafka") \
       .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
       .option("subscribe", "foobar") \
       .option("startingOffsets", "earliest") \
       .load()

raw_stream.printSchema()

 
# Define schema for the data
schema = StructType([
    StructField("VendorID", StringType()),
    StructField("pickup_datetime", StringType()),
    StructField("dropoff_datetime", StringType()),
    StructField("passenger_count", StringType()),
    StructField("trip_distance", StringType()),
    StructField("pickup_longitude", StringType()),
    StructField("pickup_latitude", StringType()),
    StructField("RateCodeID", StringType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("dropoff_longitude", StringType()),
    StructField("dropoff_latitude", StringType()),
    StructField("payment_type", StringType()),
    StructField("fare_amount", StringType()),
    StructField("extra", StringType()),
    StructField("mta_tax", StringType()),
    StructField("tip_amount", StringType()),
    StructField("tolls_amount", StringType()),
    StructField("improvement_surcharge", StringType()),
    StructField("total_amount", StringType())
])
 

   

parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_data","timestamp as kafka_timestamp") \
    .select(from_json(col("json_data"), schema).alias("data"),"kafka_timestamp") \
    .select("data.*","kafka_timestamp")   

parsed_stream.printSchema(); 

 
# # Print the schema of the parsed stream for verification
# 3. Basic Operations on Streaming DataFrames:
# 3.1 -  Apply basic transformations like select, filter, and withColumn to the streaming DataFrame.
# 3.2 - Print the results of these operations to verify their functionality. 


selected = parsed_stream.select("VendorID", "pickup_datetime", "trip_distance", "total_amount")
filtered = selected.filter(col("trip_distance").cast("double") > 2.0)


transformed = filtered.withColumn(
       "fare_per_mile",
       (col("total_amount").cast("double") / col("trip_distance").cast("double"))
   )
query = transformed.writeStream \
       .outputMode("append")\
       .format("console") \
       .option("truncate","false")\
       .start()
   
query.awaitTermination()

