# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## My first notebook

# COMMAND ----------

df_customer = spark.table('samples.tpch.customer').limit(100)
display(df_customer)
