# Databricks notebook source
from pyspark.sql import SparkSession

sourceConnectionString = "mongodb://democosmosmongodb:WFoP8mcNcPyzra4R8eilb9PH4zPvZEtgMBUdPlEzMM2yMKR7Tz22M2nuFLC4l6awetQJPhgtHEARACDbeAFhDQ==@democosmosmongodb.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@democosmosmongodb@"
sourceDb = "demomongodb"
sourceCollection =  "demomongocollection"
targetConnectionString = "mongodb://democosmosmongodb:WFoP8mcNcPyzra4R8eilb9PH4zPvZEtgMBUdPlEzMM2yMKR7Tz22M2nuFLC4l6awetQJPhgtHEARACDbeAFhDQ==@democosmosmongodb.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@democosmosmongodb@"
targetDb = "demotarget"
targetCollection =  "demotgtcollection"

my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .getOrCreate()

df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", sourceConnectionString).option("database", sourceDb).option("collection", sourceCollection).load()
df.show(5)

# COMMAND ----------

df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("uri", targetConnectionString).option("database", targetDb).option("collection", targetCollection).option("maxBatchSize", 10).save()
