# Databricks notebook source
# MAGIC %run
# MAGIC /Workspace/Repos/h20240186@pilani.bits-pilani.ac.in/spark-olist-pipeline/src/utils/config

# COMMAND ----------

from pyspark.sql.functions import col, trim, lower, upper, initcap

def curate_to_silver(spark, key, value, df=None):
    """
    Curate raw data to silver:
    - Deduplicate
    - Standardize text fields
    - Write to Delta in production
    """
    # If no df passed, read from Delta
    if df is None:
        df = spark.read.format("delta").load(value["input_path"])

    # Deduplicate
    if value.get("deduplicate_cols"):
        df = df.dropDuplicates(value["deduplicate_cols"])

    # Standardize
    if value.get("standardize_cols"):
        for col_name, rule in value["standardize_cols"].items():
            if rule == "lower":
                df = df.withColumn(col_name, lower(trim(col(col_name))))
            elif rule == "upper":
                df = df.withColumn(col_name, upper(trim(col(col_name))))
            elif rule == "title":
                df = df.withColumn(col_name, initcap(trim(col(col_name))))

    # Write in production mode
    (df.write.format("delta")
       .mode("overwrite")
       .option("maxRecordsPerFile", 1_000_000)
       .saveAsTable(key))

    return df   #so tests can assert



# COMMAND ----------

for key,value in silver_sources.items():
    curate_to_silver(key,value)

# COMMAND ----------

#Broadcasting small tables

from pyspark.sql import functions as F
from pyspark.sql.functions import *

from pyspark import StorageLevel
products    = spark.table("products_clean")                     
translations= spark.table("product_category_translation_clean") 
order_items = spark.table("order_items_clean")                  
sellers     = spark.table("sellers_clean")  



dim_products = (
    products.join(broadcast(translations), "product_category_name", "left")
)


order_items_enriched = (
    order_items.join(broadcast(dim_products), "product_id", "left")
               .join(broadcast(sellers), "seller_id", "left")
)


order_items_enriched.count()




# COMMAND ----------

#salting seller id
from pyspark.sql import functions as F

buckets = 8  
salts = spark.range(buckets).withColumnRenamed("id", "__salt")


order_items = spark.table("order_items_clean").select(
    "order_id","product_id","seller_id","price","freight_value"
)


sellers = spark.table("sellers_clean").select("seller_id","seller_city","seller_state")
sellers_salted = sellers.crossJoin(salts)  


oi_salted = order_items.withColumn(
    "__salt",
    F.pmod(F.xxhash64(F.col("order_id")), F.lit(buckets)).cast("int")
)


order_items_enriched = (
    oi_salted.join(sellers_salted, ["seller_id","__salt"], "left")
             .drop("__salt")
)


# COMMAND ----------


