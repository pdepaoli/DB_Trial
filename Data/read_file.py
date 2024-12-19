# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC select * from json.`/Workspace/Users/philip-bredby.de-paoli@capgemini.com/DB_Trial/Data/products.json`

# COMMAND ----------

import json
with open (f"Workspace/Users/philip-bredby.de-paoli@capgemini.com/DB_Trial/Data/products.json", "r") as f:
    js=json.load(f)
display(js)

# COMMAND ----------

import os
os.getcwd()

# COMMAND ----------

df = spark.read.json('products.json')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

dbutils.fs.ls('dbfs:Data/')

# COMMAND ----------

import sys
sys.path

# COMMAND ----------


