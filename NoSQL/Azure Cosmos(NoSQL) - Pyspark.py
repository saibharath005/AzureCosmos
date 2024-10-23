# Databricks notebook source
# Set configuration settings
config = {
  "spark.cosmos.accountEndpoint": "https://democosmosnosql.documents.azure.com:443/",
  "spark.cosmos.accountKey": "lRzLNbbsfXgSI4Y7cl6GAqbXITP1GgcNWqRU28MzBVjLJxDBf34rYEmWf9dazOoOHXH56ZZtprHQACDbtMW3Sg==",
  "spark.cosmos.database": "cosmicworks",
  "spark.cosmos.container": "products"
}

# # Configure Catalog Api    
spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", config["spark.cosmos.accountEndpoint"])
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", config["spark.cosmos.accountKey"])

# COMMAND ----------


spark.sql(
    "CREATE TABLE IF NOT EXISTS cosmosCatalog.cosmicworks.products "
    "USING cosmos.oltp "
    "TBLPROPERTIES(partitionKeyPath = '/category')"
)
spark.sql(
    "CREATE TABLE IF NOT EXISTS cosmosCatalog.cosmicworks.employees "
    "USING cosmos.oltp "
    "TBLPROPERTIES(partitionKeyPath = '/organization,/department,/team')"
)

# COMMAND ----------

# Create a database by using the Catalog API    
# Create a database by using the Catalog API    
spark.sql(f"CREATE DATABASE IF NOT EXISTS cosmosCatalog.cosmicworks;")
# Create a products container by using the Catalog API
spark.sql(
    "CREATE TABLE IF NOT EXISTS cosmosCatalog.cosmicworks.products "
    "USING cosmos.oltp "
    "TBLPROPERTIES(partitionKeyPath = '/category')"
)
# Create an employees container by using the Catalog API
spark.sql(
    "CREATE TABLE IF NOT EXISTS cosmosCatalog.cosmicworks.employees "
    "USING cosmos.oltp "
    "TBLPROPERTIES(partitionKeyPath = '/organization,/department,/team')"
)

# COMMAND ----------

# Create sample data    
products = (
  ("68719518391", "gear-surf-surfboards", "Yamba Surfboard", 12, 850.00, False),
  ("68719518371", "gear-surf-surfboards", "Kiama Classic Surfboard", 25, 790.00, True),
  ("1", "gear-surf-surfboards", "Yamba Surfboard", 12, 850.00, False),
  ("2", "gear-surf-surfboards", "Kiama Classic Surfboard", 25, 790.00, True)
)

# COMMAND ----------

# Ingest sample data    
spark.createDataFrame(products) \
  .toDF("id", "category", "name", "quantity", "price", "clearance") \
  .write \
  .format("cosmos.oltp") \
  .options(**config) \
  .option("spark.cosmos.database", "cosmicworks") \
  .option("spark.cosmos.container", "products")\
  .mode("APPEND") \
  .save()

# COMMAND ----------

# Load data    
df = spark.read.format("cosmos.oltp") \
  .options(**config) \
  .option("spark.cosmos.read.inferSchema.enabled", "true") \
  .load()

# COMMAND ----------

# Render schema    
df.printSchema()

# COMMAND ----------

# Render filtered data    
df.where("quantity < 20") \
  .show()

# COMMAND ----------

# Render 1 row of flitered data    
df.filter(df.clearance == True) \
  .show(1)

# COMMAND ----------

# Render five rows of unfiltered and untruncated data    
df.show(5, False)

# COMMAND ----------

# Render results of raw query    
rawQuery = "SELECT * FROM cosmosCatalog.cosmicworks.products WHERE price > 800"
rawDf = spark.sql(rawQuery)
rawDf.show()

# COMMAND ----------

# Create employee data
employees = (
  ("6347638858132", "CosmicWorks", "Marketing", "Outside Sales", "Alain Henry",  '[ { "type": "phone", "value": "425-555-0117" }, { "email": "alain@adventure-works.com" } ]'), 
)

# COMMAND ----------

# Ingest data

config = {
  "spark.cosmos.accountEndpoint": "https://democosmosnosql.documents.azure.com:443/",
  "spark.cosmos.accountKey": "lRzLNbbsfXgSI4Y7cl6GAqbXITP1GgcNWqRU28MzBVjLJxDBf34rYEmWf9dazOoOHXH56ZZtprHQACDbtMW3Sg==",
#   "spark.cosmos.database": "cosmicworks",
#   "spark.cosmos.container": "employees"
}

spark.createDataFrame(employees) \
  .toDF("id", "organization", "department", "team", "name", "contacts") \
  .write \
  .format("cosmos.oltp") \
  .options(**config) \
  .option("spark.cosmos.database" , "cosmicworks") \
  .option("spark.cosmos.container" , "employees") \
  .mode("APPEND") \
  .save()

# COMMAND ----------

# Read and render data
rawJsonDf = spark.read.format("cosmos.oltp") \
  .options(**config) \
  .option("spark.cosmos.database" , "cosmicworks") \
  .option("spark.cosmos.container" , "employees") \
  .load()
rawJsonDf.show()
