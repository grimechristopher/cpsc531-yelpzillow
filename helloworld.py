from pyspark.sql import SparkSession
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# Create a SparkSession
spark = SparkSession.builder \
    .master("local[4]") \
    .appName("HelloWorld") \
    .getOrCreate()

# Create an RDD with a single element (can be any type)
data = ["Hello, World!"]

# Parallelize the data to create an RDD
rdd = spark.sparkContext.parallelize(data)

# Perform a simple transformation: convert each element to uppercase
rdd_upper = rdd.map(lambda x: x.upper())

# Action: Collect the result back to the driver and print it
result = rdd_upper.collect()
for word in result:
    print(word)

# Stop the SparkSession
spark.stop()