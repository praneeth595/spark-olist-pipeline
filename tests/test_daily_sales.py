# Databricks notebook source
# MAGIC %run
# MAGIC /Workspace/Repos/h20240186@pilani.bits-pilani.ac.in/spark-olist-pipeline/src/gold/gold_sales
# MAGIC
# MAGIC

# COMMAND ----------

import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark_session = SparkSession.builder \
        .master("local[*]") \
        .appName("pytest-pyspark-local-testing") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()


# COMMAND ----------


from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import functions as F



def test_daily_state_revenue(spark):
    orders = spark.createDataFrame([
        ("o1", "c1", "2018-01-01 10:00:00"),
        ("o2", "c2", "2018-01-01 12:00:00"),
        ("o3", "c1", "2018-01-02 09:00:00"),
    ], ["order_id", "customer_id", "order_purchase_timestamp"])\
    .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))

    customers = spark.createDataFrame([
        ("c1", "SP"),
        ("c2", "RJ"),
    ], ["customer_id", "customer_state"])

    payments = spark.createDataFrame([
        ("o1", 100.0),
        ("o2", 200.0),
        ("o3", 150.0),
    ], ["order_id", "payment_value"])

    result = build_daily_state_revenue(orders, customers, payments)

    expected = spark.createDataFrame([
        ("2018-01-01", "SP", 100.0),
        ("2018-01-01", "RJ", 200.0),
        ("2018-01-02", "SP", 150.0),
    ], ["order_date", "customer_state", "daily_revenue"])\
    .withColumn("order_date", F.to_date("order_date"))

    assert_df_equality(result, expected, ignore_row_order=True, ignore_column_order=True)


# COMMAND ----------

# MAGIC %pip install chispa

# COMMAND ----------



# COMMAND ----------


