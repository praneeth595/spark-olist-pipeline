# Databricks notebook source
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import broadcast, to_date
from delta.tables import DeltaTable

GOLD_PATH = "/Volumes/spark_olist/gold/delta/daily_sales_state"

def load_silver(spark):
    orders    = spark.read.format("delta").load("/Volumes/spark_olist/silver/delta/orders_clean")
    customers = spark.read.format("delta").load("/Volumes/spark_olist/silver/delta/customers_clean")
    payments  = spark.read.format("delta").load("/Volumes/spark_olist/silver/delta/order_payments_clean")
    return orders, customers, payments

def build_daily_state_revenue(orders, customers, payments):
    pay_per_order = payments.groupBy("order_id").agg(F.sum("payment_value").alias("payment_total"))
    daily = (
        orders.join(broadcast(customers), "customer_id", "left")
              .join(pay_per_order, "order_id", "inner")
              .withColumn("order_date", to_date("order_purchase_timestamp"))
              .groupBy("order_date", "customer_state")
              .agg(F.sum("payment_total").alias("daily_revenue"))
    )
    return daily

def initial_write(daily_df):
    (daily_df.write.format("delta")
            .partitionBy("order_date")
            .option("maxRecordsPerFile", 50_000)
            .mode("overwrite")
            .save(GOLD_PATH))

def merge_incremental(spark):
    
    target = DeltaTable.forPath(spark, GOLD_PATH)

    
    max_date_row = spark.read.format("delta").load(GOLD_PATH)\
                      .agg(F.max("order_date").alias("max_date")).collect()[0]
    max_date = max_date_row["max_date"]

    orders, customers, payments = load_silver(spark)
    pay_per_order = payments.groupBy("order_id").agg(F.sum("payment_value").alias("payment_total"))

    incr = (
        orders.filter(F.col("order_purchase_timestamp") > F.to_timestamp(F.lit(max_date)))
              .join(customers, "customer_id", "left")
              .join(pay_per_order, "order_id", "inner")
              .withColumn("order_date", to_date("order_purchase_timestamp"))
              .groupBy("order_date", "customer_state")
              .agg(F.sum("payment_total").alias("daily_revenue"))
    )

    (target.alias("t")
           .merge(incr.alias("s"),
                  "t.order_date = s.order_date AND t.customer_state = s.customer_state")
           .whenMatchedUpdate(set={"daily_revenue": "s.daily_revenue"})
           .whenNotMatchedInsert(values={
               "order_date": "s.order_date",
               "customer_state": "s.customer_state",
               "daily_revenue": "s.daily_revenue"
           }).execute())





# COMMAND ----------

    spark = (SparkSession.builder.appName("Olist Gold Daily Sales")
             .config("spark.sql.adaptive.enabled", "true").getOrCreate())
    orders, customers, payments = load_silver(spark)
    daily = build_daily_state_revenue(orders, customers, payments)
    initial_write(daily)           # run once
    merge_incremental(spark)     # use for subsequent runs

# COMMAND ----------



if __name__ == "__main__":
    spark = SparkSession.builder.appName("Olist Optimize").getOrCreate()

    spark.sql(f"""
  OPTIMIZE delta.`{GOLD_PATH}`
  ZORDER BY (customer_state, daily_revenue)
""")

# COMMAND ----------


