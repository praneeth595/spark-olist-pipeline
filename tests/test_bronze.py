# Databricks notebook source
# MAGIC %run
# MAGIC /Workspace/Repos/h20240186@pilani.bits-pilani.ac.in/spark-olist-pipeline/src/bronze/bronze_ingest

# COMMAND ----------

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import functions as F
from pyspark.sql.functions import date_format
def test_transform_bronze_adds_partition_col(spark):
    data = [
        ("o1", "2021-01-15 10:00:00"),
        ("o2", "2021-02-20 12:00:00"),
    ]
    df = spark.createDataFrame(data, ["order_id", "order_purchase_timestamp"]) \
              .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))

    result = transform_bronze(df, "order_purchase_timestamp", repartition_rows=None)

    expected = spark.createDataFrame([
        ("o1", "2021-01-15 10:00:00", "2021-01"),
        ("o2", "2021-02-20 12:00:00", "2021-02"),
    ], ["order_id", "order_purchase_timestamp", "order_month"]) \
    .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))

    assert_df_equality(result, expected, ignore_row_order=True, ignore_column_order=True)


# COMMAND ----------



# COMMAND ----------


