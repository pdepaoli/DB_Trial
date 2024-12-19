# Databricks notebook source
# MAGIC %pip install dbutils

# COMMAND ----------

import os
cwd=os.getcwd()
print(cwd)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE SCHEMA IF NOT EXISTS TRIAL_DB_cl;
# MAGIC create schema if not exists trial_db_custom_location location '${cwd}/DB_Trial/TRIAL_DB.db';

# COMMAND ----------

# MAGIC %sql
# MAGIC drop schema trial_db_default_location;

# COMMAND ----------

import os
os.getcwd()

# COMMAND ----------

# import dbutils
uname= dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
workspace = dbutils.notebook.entry_point.getDbutils().notebook().getContext() \
.browserHostName().toString()
workspaceb

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_schema();
