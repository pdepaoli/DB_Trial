# Databricks notebook source
# MAGIC %md
# MAGIC ## SCD2 Databricks template
# MAGIC Dependencies
# MAGIC  - An insert date or updated date to use as a timestamp column
# MAGIC  - All columns propageted to target table are used in the historization logic
# MAGIC  - If multiple sourcetables are used one must calculate the correct timestamp_col to use for the scd2 build. This logic should depend on: 
# MAGIC     - Which table 

# COMMAND ----------

from pyspark.sql.functions import lead, md5, concat_ws, col, lit, when, monotonically_increasing_id, to_timestamp, nvl
from pyspark.sql import Window, DataFrame

## Concatinate columns function
def concat_columns_with_seperator(column_list: list, seperator: str = ';' ):
    return concat(*[col(c) if i == len(column_list) - 1 else concat(col(c), lit(seperator)) for i, c in enumerate(column_list)])

## Create SCD2 on historic data (initialload)
def create_historic_scd2(df: DataFrame, business_keys: list, timestamp_col, md5_cols: list, table_name: str):

    window_func = Window.partitionBy(primary_keys).orderBy(timestamp_col)
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
    .drop(*primary_keys) \
    .select(*column_sort_order)
    return target_df

## Write operation - should add a delete step first (or just an insert - unsure yet)
def write_initial_load(df: DataFrame, table_location: str, table_name: str):
    df.write.mode('overwrite').option("path", table_location).saveAsTable(table_name)
    return print(f'Table {table_name} created at {table_location}')


# COMMAND ----------

# MAGIC %md
# MAGIC Mandatory unit testing after each run

# COMMAND ----------

## Check duplicates on current key
def test_duplicate_business_key(df: DataFrame):
    filtered_df=tgt_df.filter(col("current_row") == 1)
    duplicate_keys = (
        filtered_df.groupBy("business_key")
        .agg(count("*").alias("count"))
        .filter(col("count") > 1))
    return duplicate_keys

## Check that scd2 have continous dates and no gaps
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
            (col("valid_from") > col("valid_from_lag")) &
            (col("valid_to") > col("valid_to_lead")))
    return test_df.select('business_key','valid_from','valid_from_lag','valid_to','valid_to_lead')
