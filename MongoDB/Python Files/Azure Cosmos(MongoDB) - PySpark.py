# Databricks notebook source
from pyspark.sql import SparkSession

sourceConnectionString = ""
sourceDb = "demomongodb"
sourceCollection =  "demomongocollection"
targetConnectionString = ""
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
