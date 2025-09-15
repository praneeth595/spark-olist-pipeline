# Databricks notebook source
# MAGIC %run
# MAGIC /Workspace/Repos/h20240186@pilani.bits-pilani.ac.in/spark-olist-pipeline/src/silver/silver_transform

# COMMAND ----------

from chispa.dataframe_comparer import assert_df_equality
from silver.silver_transform import curate_to_silver

def test_curate_to_silver_standardizes_and_dedupes(spark):
    data = [
        ("c1", "  sao paulo ", "sp"),
        ("c1", "  sao paulo ", "sp"),  # duplicate row
        ("c2", "RIO DE JANEIRO", "rj"),
    ]
    df = spark.createDataFrame(data, ["customer_id", "customer_city", "customer_state"])

    config = {
        "deduplicate_cols": ["customer_id"],
        "standardize_cols": {"customer_city": "title", "customer_state": "upper"},
        "input_path": "",  # unused in test
    }

    # Pass df explicitly so no Delta read happens
    result = curate_to_silver(spark, "customers_clean", config, df)

    expected = spark.createDataFrame([
        ("c1", "Sao Paulo", "SP"),
        ("c2", "Rio De Janeiro", "RJ"),
    ], ["customer_id", "customer_city", "customer_state"])

    assert_df_equality(result, expected, ignore_row_order=True, ignore_column_order=True)


# COMMAND ----------

# MAGIC %pip install chispa

# COMMAND ----------


