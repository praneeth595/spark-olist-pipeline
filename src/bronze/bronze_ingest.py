# Databricks notebook source
# MAGIC %run
# MAGIC /Workspace/Repos/h20240186@pilani.bits-pilani.ac.in/spark-olist-pipeline/src/utils/config

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

from pyspark.sql.functions import date_format

def transform_bronze(df, partition_col=None, repartition_rows=None):
    if partition_col:
        df = df.withColumn("order_month", date_format(df[partition_col], "yyyy-MM"))
        if repartition_rows:
            df = df.repartition(repartition_rows, "order_month")  # Only if explicitly requested
    return df


def ingest_to_bronze(spark, cfg):
    df = (spark.read.format("csv")
                  .option("header", True)
                  .option("inferSchema", True)
                  .load(cfg["input_path"]))
    df = transform_bronze(df, cfg.get("partition_col"))
    writer = (df.write.format("delta")
                  .option("maxRecordsPerFile", 1_000_000)
                  .mode("overwrite"))
    if cfg.get("partition_col"):
        writer = writer.partitionBy("order_month")
    writer.save(cfg["output_path"])
    return df


# COMMAND ----------

if __name__ == "__main__":
    for table, cfg in bronze_sources.items():
        ingest_to_bronze(spark, cfg)

# COMMAND ----------

#inspecting schema for transformations

def print_schema(link):
    df = spark.read.format("delta").load(link)
    df.printSchema()



for key,value in bronze_sources.items():
    print_schema(value["output_path"])

# COMMAND ----------


                                
