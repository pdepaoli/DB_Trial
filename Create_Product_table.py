# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

table_name = 'products'
tbl_loc = spark.sql(f"DESCRIBE DETAIL {table_name}").first().location
print(tbl_loc)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS default.test_table (
# MAGIC   pk bigint GENERATED ALWAYS AS IDENTITY,
# MAGIC   some_val varchar(10),
# MAGIC   another_val varchar(50)
# MAGIC );

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/products.json"
file_type = "json"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.json(file_location, multiLine=True)
display(df.head(10))

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("path", "dbfs:/user/hive/warehouse/trial_db/products").saveAsTable("trial_db.products")
