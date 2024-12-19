# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

from pyspark.sql.functions import *

# File location and type
file_name = "customer_data.json"
file_location = "/FileStore/tables/" + file_name
file_type = "json"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.json(file_location, multiLine=True)
base_df = df.select(
    "accountbalance",
    "accountgroup",
    "accountstatus",
    "customeraccount",
    "customerid",
    "customerstatus",
    "productid",
    to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss").alias("updated_at"),
)
# display(base_df)

# COMMAND ----------

natural_key = ['customerid','customeraccount']
duplicates = df.groupby(natural_key).count().where('count > 1').display()

# COMMAND ----------

df.where('customerid =="CUST00031" and customeraccount == "ACC00031"').orderBy('updated_at').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reusable functions for SCD2-logic and write operation

# COMMAND ----------

from pyspark.sql.functions import lead, md5, concat_ws, col, lit, when, monotonically_increasing_id, to_timestamp, nvl
from pyspark.sql import Window, DataFrame

def concat_columns_with_seperator(column_list: list, seperator: str = ';' ):
    return concat(*[col(c) if i == len(column_list) - 1 else concat(col(c), lit(seperator)) for i, c in enumerate(column_list)])

def create_historic_scd2(df: DataFrame, business_keys: list, timestamp_col, md5_cols: list, table_name: str):

    window_func = Window.partitionBy(business_keys).orderBy(timestamp_col)
    concat_expr_md5 = concat_columns_with_seperator(md5_cols, ';')
    concat_expr_bk = concat_columns_with_seperator(business_keys, ';')
    column_sort_order = ["PK_"+table_name]+[col for col in list(md5_cols) if col not in business_keys]+['business_key','valid_from','valid_to','scd_hash','current_row']
    target_df = df \
    .withColumn("PK_"+table_name, monotonically_increasing_id()) \
    .withColumn("business_key", concat_expr_bk) \
    .withColumn("valid_to", lead(timestamp_col).over(window_func)) \
    .withColumn("scd_hash", md5(concat_expr_md5)) \
    .withColumnRenamed("updated_at", "valid_from") \
    .withColumn('current_row', when(col('valid_to').isNull(), lit(1)).otherwise(0)) \
    .withColumn("valid_to", when(col('valid_to').isNull(), to_timestamp(lit('9999-12-31 23:59:00'),'yyyy-MM-dd HH:mm:ss')).otherwise(col('valid_to'))) \
    .drop(*business_keys) \
    .select(*column_sort_order)
    return target_df

def write_initial_load(df: DataFrame, table_location: str, table_name: str):
    df.write.format('delta').mode('overwrite').option("path", table_location).saveAsTable(table_name)
    return print(f'Table {table_name} created at {table_location}')


# COMMAND ----------

table_loc = 'dbfs:/user/hive/warehouse/trial_db/dim_customer'
table_name = 'dim_customer'
natural_key = ['customerid','customeraccount']
timestamp_col = 'updated_at'
tgt_df = create_historic_scd2(base_df, natural_key, timestamp_col, md5_cols,table_name)
# tgt_df.display()
# tgt_df.write.format('delta').mode('overwrite').saveAsTable('trial_db.'+table_name)

# COMMAND ----------


def test_duplicate_business_key(df: DataFrame):
    filtered_df=tgt_df.filter(col("current_row") == 1)
    duplicate_keys = (
        filtered_df.groupBy("business_key")
        .agg(count("*").alias("count"))
        .filter(col("count") > 1))
    return duplicate_keys

def test_scd2_date_continous(df: DataFrame):
    window_func = Window.partitionBy(["business_key"]).orderBy(["valid_from"])
    test_df = tgt_df \
        .withColumn("valid_from_lag", lag("valid_from").over(window_func)) \
        .withColumn("valid_from_lag", when(col('valid_from_lag').isNull(), to_timestamp(lit('1900-01-01 00:00:00'),'yyyy-MM-dd  HH:mm:ss')).otherwise(col("valid_from_lag"))) \
        .withColumn("valid_to_lead", lead("valid_to").over(window_func)) \
        .withColumn("valid_to_lead", when(col('valid_to_lead').isNull(), to_timestamp(lit('9999-12-31 23:59:00'),'yyyy-MM-dd    HH:mm:ss')).otherwise(col("valid_to_lead"))) \
        .filter(
            (col("valid_from") > col("valid_to")) &
            (col("valid_from") < col("valid_from_lag")) &
            (col("valid_to") > col("valid_to_lead")))
    return test_df.select('business_key','valid_from','valid_from_lag','valid_to','valid_to_lead')

# COMMAND ----------



# COMMAND ----------

def create_historic_scd2(df: DataFrame, business_keys: list, timestamp_col, md5_cols: list, table_name: str):

    window_func = Window.partitionBy(business_keys).orderBy(timestamp_col)
    concat_expr_md5 = concat_columns_with_seperator(md5_cols, ';')
    concat_expr_bk = concat_columns_with_seperator(business_keys, ';')
    column_sort_order = ["PK_"+table_name]+[col for col in list(md5_cols) if col not in business_keys]+['business_key','valid_from','valid_to','scd_hash','current_row']
    target_df = df \
    .withColumn("PK_"+table_name, monotonically_increasing_id()) \
    .withColumn("business_key", concat_expr_bk) \
    .withColumn("valid_to", lead(timestamp_col).over(window_func)) \
    .withColumn("scd_hash", md5(concat_expr_md5)) \
    .withColumnRenamed("updated_at", "valid_from") \
    .withColumn('current_row', when(col('valid_to').isNull(), lit(1)).otherwise(0)) \
    .withColumn("valid_to", when(col('valid_to').isNull(), to_timestamp(lit('9999-12-31 23:59:00'),'yyyy-MM-dd HH:mm:ss')).otherwise(col('valid_to'))) \
    .drop(*business_keys) \
    .select(*column_sort_order)
    return target_df

# COMMAND ----------

## HISTORIC SCD2 LOAD

primary_keys=['customerid','customeraccount']
timestamp_col = 'updated_at'
md5_cols = ['accountbalance','accountgroup','accountstatus','customerstatus','productid']
table_name = 'customers'
tgt_df = create_historic_scd2(base_df, primary_keys, timestamp_col, md5_cols,table_name)
business_key = ['customerid','customeraccount']
concat_expr_bk = concat_columns_with_seperator(business_key, ';')
incremental_load_df = base_df.filter(col("updated_at")>max_load_date).withColumn("business_key", concat_expr_bk)
max_load_date = tgt_df.select(max(col('valid_from'))).collect()[0][0]
# max_load_date
insert_rows_df = inc_df.select("business_key").exceptAll(tgt_df.select("business_key"))
new_rows_df.display()
# tgt_df.where("valid_from<='2024-12-12'")


# COMMAND ----------

ls
