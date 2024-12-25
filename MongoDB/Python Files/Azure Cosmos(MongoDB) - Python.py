# Databricks notebook source
import pymongo
client = pymongo.MongoClient("")
database = client["demomongodb"]
collection = database["demomongotest"]
document = {
        "projectId": "0123456789",
        "title": "Hello",
        "message":"Writing"
    }
result = collection.insert_one(document)

# COMMAND ----------

data = list(collection.find())
data
