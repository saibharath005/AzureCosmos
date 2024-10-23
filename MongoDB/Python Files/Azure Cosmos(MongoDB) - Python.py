# Databricks notebook source
import pymongo
client = pymongo.MongoClient("mongodb://democosmosmongodb:WFoP8mcNcPyzra4R8eilb9PH4zPvZEtgMBUdPlEzMM2yMKR7Tz22M2nuFLC4l6awetQJPhgtHEARACDbeAFhDQ==@democosmosmongodb.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@democosmosmongodb@")
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
