import pyspark
from pyspark.rdd import RDD
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions
from pyspark.sql.functions import (
    lit, 
    desc, 
    col, 
    size, 
    array_contains, 
    isnan, 
    udf, 
    hour, 
    array_min, 
    array_max, 
    countDistinct
)


MAX_MEMORY = '15G'
conf = pyspark.SparkConf().setMaster("local[*]") \
        .set('spark.executor.heartbeatInterval', 10000) \
        .set('spark.network.timeout', 10000) \
        .set("spark.core.connection.ack.wait.timeout", "3600") \
        .set("spark.executor.memory", MAX_MEMORY) \
        .set("spark.driver.memory", MAX_MEMORY) \
        .set('spark.cassandra.connection.host', '127.0.0.1') \
        .set('spark.cassandra.connection.port', '9042') \
        .set('spark.cassandra.output.consistency.level','ONE')
        
spark = SparkSession \
        .builder \
        .appName("Analysis") \
        .config(conf=conf) \
        .getOrCreate()
        
df = spark.read.format("org.apache.spark.sql.cassandra").options(table="posts", keyspace="reddit").load()
        
print(df)