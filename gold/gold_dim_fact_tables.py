# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp

# COMMAND ----------

@dp.table(name="globalmart_dev.gold.gold_dim_date")
def dim_date():
    return (
        spark.read.table("globalmart_dev.silver.orders_silver")
        .select(col("order_purchase_date").alias("date"))
        .distinct()
        .withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast("int"))
        .withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("day", dayofmonth(col("date")))
    )

# COMMAND ----------

# DBTITLE 1,dim region
@dp.table(name="globalmart_dev.gold.gold_dim_region")
def dim_region():
    return (
        spark.read.table("globalmart_dev.silver.customers_silver")
        .select("region")
        .distinct()
        .withColumn("region_key", monotonically_increasing_id())
        .withColumnRenamed("region", "source_region")
    )

# COMMAND ----------

# DBTITLE 1,dim product
@dp.table(name="globalmart_dev.gold.gold_dim_product")
def dim_product():
    return (
        spark.read.table("globalmart_dev.silver.products_silver")
        .dropDuplicates(["product_id"])
        .withColumn("product_key", monotonically_increasing_id())
    )

# COMMAND ----------

# DBTITLE 1,dim vendor
@dp.table(name="globalmart_dev.gold.gold_dim_vendor")
def dim_vendor():
    return (
        spark.read.table("globalmart_dev.silver.vendors_silver")
        .dropDuplicates(["vendor_id"])
        .withColumn("vendor_key", monotonically_increasing_id())
    )

# COMMAND ----------

# DBTITLE 1,dim customer
@dp.table(name="globalmart_dev.gold.gold_dim_customer")
def dim_customer():

    from pyspark.sql.functions import col, row_number, lit
    from pyspark.sql.window import Window
    from pyspark.sql.types import StructType, StructField, LongType, StringType

    base_df = (
        spark.read.table("globalmart_dev.silver.customers_silver")
        .filter(col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
    )

    # Stable surrogate key using row_number
    window_spec = Window.orderBy("customer_id")

    dim_df = base_df.withColumn(
        "customer_key",
        row_number().over(window_spec)
    )

    # ✅ ADDING new columns (no change to existing logic)
    dim_df = dim_df.select(
        "customer_key",
        "customer_id",

        col("customer_name"),
        col("customer_email"),

        col("segment").alias("customer_segment"),
        col("region"),
        col("country"),

        col("city"),
        col("state"),

        col("source_region")
    )

    # Add UNKNOWN record with explicit schema
    unknown_schema = StructType([
        StructField("customer_key", LongType(), False),
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("customer_email", StringType(), True),
        StructField("customer_segment", StringType(), False),
        StructField("region", StringType(), False),
        StructField("country", StringType(), False),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("source_region", StringType(), False)
    ])
    
    unknown_df = spark.createDataFrame(
        [(
            -1,
            "UNKNOWN",
            "UNKNOWN",
            None,
            "UNKNOWN",
            "UNKNOWN",
            "UNKNOWN",
            None,
            None,
            "UNKNOWN"
        )],
        schema=unknown_schema
    )

    # Final union
    final_df = dim_df.unionByName(unknown_df)

    return final_df

# COMMAND ----------

# DBTITLE 1,fact transactions
from pyspark.sql.functions import col, when, date_format, datediff
import dlt

# ============================================================
# FACT TRANSACTIONS
# ============================================================
@dlt.table(
    name="globalmart_dev.gold.gold_fact_transactions_mv",
    comment="Gold fact table for sales transactions"
)
def fact_transactions():

    dim_product  = dlt.read("globalmart_dev.gold.gold_dim_product").drop("source_region")
    dim_vendor   = dlt.read("globalmart_dev.gold.gold_dim_vendor").drop("source_region")
    dim_customer = dlt.read("globalmart_dev.gold.gold_dim_customer")
    dim_date     = dlt.read("globalmart_dev.gold.gold_dim_date")
    dim_region   = dlt.read("globalmart_dev.gold.gold_dim_region").select("region_key", "source_region")

    txn_df    = spark.read.table("globalmart_dev.silver.transactions_silver")
    orders_df = spark.read.table("globalmart_dev.silver.orders_silver")

    fact_base = txn_df.filter(col("sales") > 0).drop("source_region")

    fact_orders = (
        fact_base
        .join(orders_df, "order_id", "left")
        .withColumnRenamed("source_region", "order_region")
    )

    fact_product = fact_orders.join(dim_product, "product_id", "left")
    fact_vendor  = fact_product.join(dim_vendor, "vendor_id", "left")

    fact_customer = (
        fact_vendor
        .join(dim_customer, "customer_id", "left")
        .withColumn(
            "customer_key",
            when(col("customer_key").isNull(), -1).otherwise(col("customer_key"))
        )
    )

    fact_date = (
        fact_customer
        .withColumn(
            "date_key",
            date_format(col("order_purchase_date"), "yyyyMMdd").cast("int")
        )
        .join(dim_date, "date_key", "left")
    )

    fact_region = (
        fact_date
        .join(
            dim_region,
            col("order_region") == dim_region["source_region"],
            "left"
        )
        .drop("source_region")
    )

    fact_flagged = fact_region.withColumn(
        "order_link_flag",
        col("order_purchase_date").isNotNull()
    )

    return fact_flagged.select(
        "order_id",
        "customer_key",
        "product_key",
        "vendor_key",
        "date_key",
        "region_key",
        col("sales").alias("sales_amount"),
        "order_link_flag"
    )

# ============================================================
# FACT RETURNS (FRAUD USE CASE)
# ============================================================
@dlt.table(
    name="globalmart_dev.gold.gold_fact_returns",
    comment="Gold fact table for returns with fraud detection (with customer_id)"
)
def fact_returns():
    dim_product  = dlt.read("globalmart_dev.gold.gold_dim_product").drop("source_region")
    dim_vendor   = dlt.read("globalmart_dev.gold.gold_dim_vendor").drop("source_region")
    dim_customer = dlt.read("globalmart_dev.gold.gold_dim_customer")
    dim_date     = dlt.read("globalmart_dev.gold.gold_dim_date")
    dim_region   = dlt.read("globalmart_dev.gold.gold_dim_region").select("region_key", "source_region")

    returns_df = spark.readStream.table("globalmart_dev.silver.returns_silver")
    orders_df  = spark.read.table("globalmart_dev.silver.orders_silver")
    txn_df     = spark.read.table("globalmart_dev.silver.transactions_silver").drop("source_region")

    returns_orders = (
        returns_df
        .join(orders_df, "order_id", "left")
        .withColumnRenamed("source_region", "order_region")
    )

    returns_txn = returns_orders.join(txn_df, "order_id", "left")
    returns_product = returns_txn.join(dim_product, "product_id", "left")
    returns_vendor  = returns_product.join(dim_vendor, "vendor_id", "left")

    returns_customer = (
        returns_vendor
        .join(dim_customer, "customer_id", "left")
        .withColumn(
            "customer_key",
            when(col("customer_key").isNull(), -1).otherwise(col("customer_key"))
        )
    )

    returns_date = (
        returns_customer
        .withColumn(
            "date_key",
            date_format(col("return_date"), "yyyyMMdd").cast("int")
        )
        .join(dim_date, "date_key", "left")
    )

    returns_region = (
        returns_date
        .join(
            dim_region,
            col("order_region") == dim_region["source_region"],
            "left"
        )
        .drop("source_region")
    )

    returns_flagged = (
        returns_region
        .withColumn("is_valid_return", col("refund_amount") > 0)
        .withColumn("has_transaction_flag", col("sales").isNotNull())
        .withColumn("missing_txn_flag", col("sales").isNull())
        .withColumn("has_order_flag", col("order_id").isNotNull())
        .withColumn("return_without_order_flag", col("order_id").isNull())
        .withColumn("high_return_flag", col("refund_amount") > 1000)
        .withColumn(
            "return_to_purchase_ratio",
            when(col("sales").isNotNull(), col("refund_amount") / col("sales"))
        )
        .withColumn(
            "suspicious_ratio_flag",
            col("return_to_purchase_ratio") > 1.2
        )
        .withColumn(
            "days_to_return",
            datediff(col("return_date"), col("order_purchase_date"))
        )
        .withColumn(
            "quick_return_flag",
            col("days_to_return") <= 2
        )
        .withColumn(
            "data_quality_flag",
            when(col("refund_amount").isNull(), "MISSING_AMOUNT")
            .when(col("refund_amount") <= 0, "INVALID_AMOUNT")
            .when(col("customer_key") == -1, "MISSING_CUSTOMER")
            .otherwise("VALID")
        )
    )

    return returns_flagged.select(
        "order_id",
        "customer_id",
        "customer_key",
        "product_key",
        "vendor_key",
        "date_key",
        "region_key",
        col("refund_amount").alias("return_amount"),
        col("sales").alias("transaction_amount"),
        "order_purchase_date",
        "return_date",
        "return_reason",
        "has_order_flag",
        "has_transaction_flag",
        "missing_txn_flag",
        "high_return_flag",
        "is_valid_return",
        "data_quality_flag",
        "return_to_purchase_ratio",
        "suspicious_ratio_flag",
        "return_without_order_flag",
        "days_to_return",
        "quick_return_flag"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC FACT RETURNS