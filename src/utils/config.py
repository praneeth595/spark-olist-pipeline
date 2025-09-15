# Databricks notebook source
bronze_sources = {
    "customers": {
        "input_path": "/Volumes/spark_olist/data/raw/olist_customers_dataset.csv",
        "output_path": "/Volumes/spark_olist/bronze/delta/customers",
        "partition_col": None
    },
    "geolocation": {
        "input_path": "/Volumes/spark_olist/data/raw/olist_geolocation_dataset.csv",
        "output_path": "/Volumes/spark_olist/bronze/delta/geolocation",
        "partition_col": None
    },
    "order_items": {
        "input_path": "/Volumes/spark_olist/data/raw/olist_order_items_dataset.csv",
        "output_path": "/Volumes/spark_olist/bronze/delta/order_items",
        "partition_col": None
    },
    "order_payments": {
        "input_path": "/Volumes/spark_olist/data/raw/olist_order_payments_dataset.csv",
        "output_path": "/Volumes/spark_olist/bronze/delta/order_payments",
        "partition_col": None
    },
    "orders": {
        "input_path": "/Volumes/spark_olist/data/raw/olist_orders_dataset.csv",
        "output_path": "/Volumes/spark_olist/bronze/delta/orders",
        "partition_col": "order_purchase_timestamp"
    },
    "products": {
        "input_path": "/Volumes/spark_olist/data/raw/olist_products_dataset.csv",
        "output_path": "/Volumes/spark_olist/bronze/delta/products",
        "partition_col": None
    },
    "sellers": {
        "input_path": "/Volumes/spark_olist/data/raw/olist_sellers_dataset.csv",
        "output_path": "/Volumes/spark_olist/bronze/delta/sellers",
        "partition_col": None
    },
    "product_category_translation": {
        "input_path": "/Volumes/spark_olist/data/raw/product_category_name_translation.csv",
        "output_path": "/Volumes/spark_olist/bronze/delta/product_category_translation",
        "partition_col": None
    }
  
}


# COMMAND ----------

silver_sources = {
    "customers_clean": {
        "input_path": "/Volumes/spark_olist/bronze/delta/customers",
        "output_path": "/Volumes/spark_olist/silver/delta/customers_clean",
        "deduplicate_cols": ["customer_id"],
        "standardize_cols": {
            "customer_city": "title",
            "customer_state": "upper"
        }
    },
    "sellers_clean": {
        "input_path": "/Volumes/spark_olist/bronze/delta/sellers",
        "output_path": "/Volumes/spark_olist/silver/delta/sellers_clean",
        "deduplicate_cols": ["seller_id"],
        "standardize_cols": {
            "seller_city": "title",
            "seller_state": "upper"
        }
    },
    "orders_clean": {
        "input_path": "/Volumes/spark_olist/bronze/delta/orders",
        "output_path": "/Volumes/spark_olist/silver/delta/orders_clean",
        "deduplicate_cols": None,   # or ["order_id"] if you want to be safe
        "standardize_cols": {
            "order_status": "lower"
        }
    },

    # ✅ NEW: order_items
    "order_items_clean": {
        "input_path": "/Volumes/spark_olist/bronze/delta/order_items",
        "output_path": "/Volumes/spark_olist/silver/delta/order_items_clean",
        "deduplicate_cols": ["order_id", "order_item_id"],
        "standardize_cols": None  # nothing to standardize here
    },

    "products_clean": {
        "input_path": "/Volumes/spark_olist/bronze/delta/products",
        "output_path": "/Volumes/spark_olist/silver/delta/products_clean",
        "deduplicate_cols": None,   # or ["product_id"] if you want to be safe
        "standardize_cols": {
            "product_category_name": "lower"
        }
    },
    "order_payments_clean": {
        "input_path": "/Volumes/spark_olist/bronze/delta/order_payments",
        "output_path": "/Volumes/spark_olist/silver/delta/order_payments_clean",
        "deduplicate_cols": None,   # or ["order_id","payment_sequential"]
        "standardize_cols": {
            "payment_type": "lower"
        }
    },
    "geolocation_clean": {
        "input_path": "/Volumes/spark_olist/bronze/delta/geolocation",
        "output_path": "/Volumes/spark_olist/silver/delta/geolocation_clean",
        "deduplicate_cols": None,
        "standardize_cols": {
            "geolocation_city": "title",
            "geolocation_state": "upper"
        }
    },

    # 🔧 Updated: also lower the Portuguese name to match products join
    "product_category_translation_clean": {
        "input_path": "/Volumes/spark_olist/bronze/delta/product_category_translation",
        "output_path": "/Volumes/spark_olist/silver/delta/product_category_translation_clean",
        "deduplicate_cols": None,
        "standardize_cols": {
            "product_category_name": "lower",
            "product_category_name_english": "lower"
        }
    }
}
