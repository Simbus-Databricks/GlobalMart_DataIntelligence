# Databricks notebook source
# DBTITLE 1,Bronze Layer Exploratory Analysis
# MAGIC %md
# MAGIC # GlobalMart — Bronze Layer Exploratory Analysis
# MAGIC
# MAGIC **Purpose:** Systematically analyze all Bronze tables to understand data quality issues, schema variations, and business logic patterns before designing the Silver layer transformation logic.
# MAGIC
# MAGIC **Context:** This notebook sits between Bronze ingestion and Silver transformation. It profiles raw data to:
# MAGIC * Detect schema inconsistencies across regional source files
# MAGIC * Identify data quality issues (nulls, formats, invalid values)
# MAGIC * Discover cross-region duplicates and referential integrity violations
# MAGIC * Guide Silver layer quarantine rules and normalization logic
# MAGIC
# MAGIC **Tables Analyzed:**
# MAGIC * `globalmart_dev.bronze.customers` — customer master data from 6 regional systems
# MAGIC * `globalmart_dev.bronze.orders` — order transactions from 3 regional systems
# MAGIC * `globalmart_dev.bronze.transactions` — order line items from 3 regional systems
# MAGIC * `globalmart_dev.bronze.returns` — return records from 2 regional systems
# MAGIC * `globalmart_dev.bronze.products` — product catalog (single global reference)
# MAGIC * `globalmart_dev.bronze.vendors` — vendor reference (single global reference)
# MAGIC
# MAGIC **Key Findings Summary (to be updated after running analysis):**
# MAGIC * Schema variations: customers use 4 different ID column names across regions
# MAGIC * Data quality: transactions from R3 have Order_ID/Product_ID in _rescued_data
# MAGIC * Business logic: R4 has city/state columns swapped in source
# MAGIC * Referential integrity: orphaned orders, transactions, and returns detected

# COMMAND ----------

# DBTITLE 1,Import PySpark Functions and Types
# Import PySpark SQL functions, types, and window functions for data analysis
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Helper Functions for Table References and Section Headers
# Helper functions to build fully qualified table names and format section headers

# Fully qualified table references
CATALOG = "globalmart_dev"
SCHEMA  = "bronze"

def tbl(name):
    """Build fully qualified table name: catalog.schema.table"""
    return f"{CATALOG}.{SCHEMA}.{name}"

def section(title):
    """Print a formatted section header for console output"""
    print(f"\n{'─'*72}\n  {title}\n{'─'*72}")

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Table Profiling Function: Schema, Nulls, Cardinality, and Rescued Data
def profile_table(table_name):
    """Profile a Bronze table: show schema, null counts, distinct values, and rescued data stats.
    
    This function provides a comprehensive view of each column:
    - Data type as inferred by Auto Loader
    - Null count and percentage
    - Distinct value count (cardinality)
    - Flags for: 100% null, mostly null, unique key candidates, constants, rescued data
    """
    df    = spark.table(tbl(table_name))
    total = df.count()
    rescued_total = (
        df.where(F.col("_rescued_data").isNotNull()).count()
        if "_rescued_data" in df.columns else 0
    )

    print(f"\n{'='*72}")
    print(f"  TABLE : {table_name}")
    print(f"  ROWS  : {total:,}   |   ROWS WITH RESCUED DATA: {rescued_total:,}")
    print(f"{'='*72}")
    print(f"  {'COLUMN':<38} {'SPARK TYPE':<14} {'NULLS':>8} {'NULL%':>7} {'DISTINCT':>10}")
    print(f"  {'─'*38} {'─'*14} {'─'*8} {'─'*7} {'─'*10}")

    for field in df.schema.fields:
        col_name  = field.name
        col_type  = type(field.dataType).__name__.replace("Type","")
        null_cnt  = df.where(F.col(col_name).isNull()).count()
        null_pct  = round(null_cnt / total * 100, 1) if total > 0 else 0
        distinct  = df.select(col_name).distinct().count()

        flag = ""
        if null_pct == 100:
            flag = " ◄ 100% NULL"
        elif null_pct > 80:
            flag = " ◄ mostly null"
        elif distinct == total and null_cnt == 0:
            flag = " ◄ unique — candidate key"
        elif distinct == 1:
            flag = " ◄ single value — constant or bug"
        elif col_name == "_rescued_data" and null_cnt < total:
            flag = f" ◄ {total - null_cnt:,} rows rescued — type mismatch in source"

        print(f"  {col_name:<38} {col_type:<14} {null_cnt:>8,} {null_pct:>6.1f}% {distinct:>10,}{flag}")

    print()
    return df

# COMMAND ----------

# DBTITLE 1,Profile All Bronze Tables
# Profile each Bronze table and store DataFrames for further analysis
df_customers    = profile_table("customers")
df_orders       = profile_table("orders")
df_transactions = profile_table("transactions")
df_returns      = profile_table("returns")
df_products     = profile_table("products")
df_vendors      = profile_table("vendors")

# COMMAND ----------

# DBTITLE 1,Source File Breakdown: Rows per File and Ingestion Timestamps
# Analyze source file distribution for all tables
# Shows which regional files contributed data, row counts, and ingestion times

section("2.1  Source file breakdown — all tables")

for name, df in [
    ("customers",    df_customers),
    ("orders",       df_orders),
    ("transactions", df_transactions),
    ("returns",      df_returns),
    ("products",     df_products),
    ("vendors",      df_vendors),
]:
    print(f"\n── {name} ──")
    df.groupBy("_source_file_name", "source_region") \
      .agg(
          F.count("*").alias("rows"),
          F.min("_ingested_at").alias("first_ingested"),
          F.max("_ingested_at").alias("last_ingested"),
      ) \
      .orderBy("_source_file_name") \
      .show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Rescued Data Detection: Which Tables and Files Have Type Mismatches
# Identify which tables have rescued data (Auto Loader schema inference failures)
# Rescued data = columns that couldn't be parsed into the inferred schema

section("3.1  Rescued data — which tables and which files are affected")

for name, df in [
    ("customers",    df_customers),
    ("orders",       df_orders),
    ("transactions", df_transactions),
    ("returns",      df_returns),
    ("products",     df_products),
    ("vendors",      df_vendors),
]:
    if "_rescued_data" not in df.columns:
        continue
    rescued = df.where(F.col("_rescued_data").isNotNull())
    cnt = rescued.count()
    if cnt == 0:
        print(f"  {name}: no rescued rows")
        continue

    print(f"\n── {name}: {cnt:,} rescued rows ──")
    rescued.groupBy("_source_file_name") \
           .agg(F.count("*").alias("rescued_rows")) \
           .show(truncate=False)


# COMMAND ----------

# DBTITLE 1,Rescued Data Content: Extract JSON Keys to Identify Lost Columns
# Parse rescued JSON to discover which column values were lost
# Each key in rescued JSON represents a column that failed type inference

section("3.2  Rescued data — what JSON keys are inside? (these are the lost columns)")
# Parsed rescued JSON and extract its keys.
# Every key here is a column value that failed type inference and needs to be recovered in Silver via get_json_object().

for name, df in [
    ("customers",    df_customers),
    ("transactions", df_transactions),
    ("returns",      df_returns),
]:
    rescued = df.where(F.col("_rescued_data").isNotNull())
    if rescued.count() == 0:
        continue

    print(f"\n── {name} — sample rescued JSON payloads ──")
    rescued.select("_source_file_name", "_rescued_data") \
           .show(5, truncate=False)

    # Extract all keys that appear inside the rescued JSON.
    sample_json = rescued.select("_rescued_data").limit(1).collect()[0][0]
    print(f"  Inferred schema of rescued JSON: {spark.range(1).select(F.schema_of_json(F.lit(sample_json))).collect()[0][0]}")
    print()


# COMMAND ----------

# DBTITLE 1,Rescued Data Recovery Rate: Test if Values Can Be Parsed Back
# Test whether rescued values can be successfully recovered via JSON parsing
# Unrecoverable rows = data loss, must be quarantined in Silver

section("3.3  Rescued data — can we always recover the value?")
# For each rescued column, parse it back and count how many
# succeed vs fail. Fail count = unrecoverable rows → quarantine in Silver.

# transactions: Order_ID and Product_ID
txn_rescued = df_transactions.where(F.col("_rescued_data").isNotNull()) \
    .withColumn("parsed", F.from_json(F.col("_rescued_data"),
        "Order_ID STRING, Product_ID STRING, _file_path STRING"))

print("transactions — rescued Order_ID recovery rate:")
txn_rescued.agg(
    F.count("*").alias("total_rescued"),
    F.count(F.when(F.col("parsed.Order_ID").isNotNull(),   1)).alias("Order_ID_recovered"),
    F.count(F.when(F.col("parsed.Product_ID").isNotNull(), 1)).alias("Product_ID_recovered"),
    F.count(F.when(F.col("parsed.Order_ID").isNull(),      1)).alias("UNRECOVERABLE"),
).show(truncate=False)

# returns: refund_amount arrives as "$130.65" — strip "$" and cast
ret_rescued = df_returns.where(F.col("_rescued_data").isNotNull()) \
    .withColumn("parsed", F.from_json(F.col("_rescued_data"),
        "refund_amount STRING, _file_path STRING"))

print("returns — rescued refund_amount recovery rate (after stripping '$'):")
ret_rescued.withColumn(
    "refund_clean",
    F.regexp_replace(F.col("parsed.refund_amount"), r"[$,]", "").cast(T.DoubleType())
).agg(
    F.count("*").alias("total_rescued"),
    F.count(F.when(F.col("refund_clean").isNotNull(), 1)).alias("recovered_as_double"),
    F.count(F.when(F.col("refund_clean").isNull(),    1)).alias("UNRECOVERABLE"),
).show(truncate=False)


# COMMAND ----------

# DBTITLE 1,Customer Schema Variance: Which ID, Email, Name, Segment Columns Per Region
# Map which column names each regional file uses for customer IDs, emails, names, and segments
# This guides COALESCE logic in Silver to unify schema variants

section("4.1  customers — which ID column does each source file use?")
# If a region uses customer_identifier but not customer_id, COALESCE must
# include customer_identifier. This cell tells us the exact mapping.

df_customers.groupBy("_source_file_name").agg(
    F.count("*").alias("total"),
    F.count(F.when(F.col("customer_id").isNotNull(),         1)).alias("customer_id"),
    F.count(F.when(F.col("CustomerID").isNotNull(),          1)).alias("CustomerID"),
    F.count(F.when(F.col("cust_id").isNotNull(),             1)).alias("cust_id"),
    F.count(F.when(F.col("customer_identifier").isNotNull(), 1)).alias("customer_identifier"),
).orderBy("_source_file_name").show(truncate=False)

# ─────────────────────────────────────────────────────────────────────────────
section("4.2  customers — which email / name / segment column does each file use?")

df_customers.groupBy("_source_file_name").agg(
    F.count("*").alias("total"),
    # email
    F.count(F.when(F.col("customer_email").isNotNull(), 1)).alias("customer_email"),
    F.count(F.when(F.col("email_address").isNotNull(),  1)).alias("email_address"),
    F.count(F.when(
        F.col("customer_email").isNull() & F.col("email_address").isNull(), 1
    )).alias("NO_email"),
    # name
    F.count(F.when(F.col("customer_name").isNotNull(), 1)).alias("customer_name"),
    F.count(F.when(F.col("full_name").isNotNull(),     1)).alias("full_name"),
    # segment
    F.count(F.when(F.col("segment").isNotNull(),          1)).alias("segment"),
    F.count(F.when(F.col("customer_segment").isNotNull(), 1)).alias("customer_segment"),
).orderBy("_source_file_name").show(truncate=False)


# COMMAND ----------

# DBTITLE 1,Returns Schema Variance: Column Name Mapping for Two Different File Formats
# Returns come from two completely different schema formats
# Map which file uses which column names so Silver can COALESCE correctly

section("4.3  returns — which columns does each source file use?")
# returns_1.json and returns_2.json have completely different schemas.
# This cell maps exactly which file uses which column name so Silver
# can COALESCE the right pairs.

df_returns.groupBy("_source_file_name").agg(
    F.count("*").alias("total"),
    # order id
    F.count(F.when(F.col("OrderId").isNotNull(),        1)).alias("OrderId"),
    F.count(F.when(F.col("order_id").isNotNull(),       1)).alias("order_id"),
    # amount
    F.count(F.when(F.col("amount").isNotNull(),         1)).alias("amount"),
    F.count(F.when(F.col("refund_amount").isNotNull(),  1)).alias("refund_amount"),
    # date
    F.count(F.when(F.col("date_of_return").isNotNull(), 1)).alias("date_of_return"),
    F.count(F.when(F.col("return_date").isNotNull(),    1)).alias("return_date"),
    # reason
    F.count(F.when(F.col("reason").isNotNull(),         1)).alias("reason"),
    F.count(F.when(F.col("return_reason").isNotNull(),  1)).alias("return_reason"),
    # status
    F.count(F.when(F.col("status").isNotNull(),         1)).alias("status"),
    F.count(F.when(F.col("return_status").isNotNull(),  1)).alias("return_status"),
).orderBy("_source_file_name").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Transaction Schema Variance: Order_ID vs Order_id Casing Issue
# Transactions from R3 have Order_ID (capital D) vs other regions with Order_id (lowercase d)
# Auto Loader treated these as different columns and rescued one variant

section("4.4  transactions — which Order / Product ID column does each file use?")
# transactions_3.csv has Order_ID (capital D) which clashed with Order_id
# (lowercase d) from other files — Auto Loader treated them as different columns
# and rescued one into _rescued_data. This cell confirms the mapping.

df_transactions.groupBy("_source_file_name").agg(
    F.count("*").alias("total"),
    F.count(F.when(F.col("Order_id").isNotNull(),   1)).alias("Order_id"),
    F.count(F.when(F.col("Order_ID").isNotNull(),   1)).alias("Order_ID_if_exists"),
    F.count(F.when(F.col("Product_id").isNotNull(), 1)).alias("Product_id"),
    F.count(F.when(F.col("_rescued_data").isNotNull(), 1)).alias("rescued_rows"),
).orderBy("_source_file_name").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Customer Segment Values: Detect Abbreviations and Typos for Normalization
# Collect all distinct segment values across both segment columns
# Small-count values indicate abbreviations or typos that need normalization in Silver

section("5.1  customers — all distinct segment values across BOTH columns")
# We collect distinct values from both segment columns combined.
# Any value with count < 5% of the total is a candidate abbreviation or typo.

total_rows = df_customers.where(
    F.coalesce(F.col("segment"), F.col("customer_segment")).isNotNull()
).count()

df_customers.select(
    F.coalesce(F.col("segment"), F.col("customer_segment")).alias("raw_segment"),
    "_source_file_name"
).where(F.col("raw_segment").isNotNull()) \
 .groupBy("raw_segment") \
 .agg(
     F.count("*").alias("count"),
     (F.count("*") / total_rows * 100).alias("pct_of_total"),
     F.collect_set("_source_file_name").alias("source_files")
 ) \
 .orderBy(F.desc("count")) \
 .show(50, truncate=False)


# COMMAND ----------

# DBTITLE 1,Customer City/State Swap Detection: Find Files Where City Contains State Names
# Detect column swap bug: check if 'city' column contains US state names
# This indicates the source file has city and state columns physically swapped

section("5.2  customers — are city and state swapped in any file?")
# Strategy: build a reference set of known US state names.
# Then check if the 'city' column contains values that are actually state names.
# Any file where city contains state names has a column swap bug.

known_states = [
    "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut",
    "Delaware","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa",
    "Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan",
    "Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada",
    "New Hampshire","New Jersey","New Mexico","New York","North Carolina",
    "North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island",
    "South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont",
    "Virginia","Washington","West Virginia","Wisconsin","Wyoming"
]

print("Files where 'city' column contains a US state name (swap detected):")
df_customers \
    .where(F.col("city").isin(known_states)) \
    .groupBy("_source_file_name") \
    .agg(
        F.count("*").alias("rows_with_state_in_city_col"),
        F.collect_set("city").alias("sample_state_values_in_city"),
    ) \
    .show(truncate=False)


# COMMAND ----------

# DBTITLE 1,Order Date Format Patterns: Classify Every Date Column by Format
# Classify date format patterns in every order date column
# Don't assume formats—use regex to discover what's actually in the data

section("5.3  orders — what date format patterns exist in each date column?")
# We don't assume the format. We regex-classify every value and count per file.
# Any format that is not one of the two main patterns is a new problem.

date_cols = [
    "order_purchase_date", "order_approved_at",
    "order_delivered_carrier_date", "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

def classify_date(col_name):
    return (
        F.when(F.col(col_name).isNull(), "NULL")
         .when(F.col(col_name).rlike(r"^\d{2}/\d{2}/\d{4}"),  "MM/DD/YYYY HH:mm")
         .when(F.col(col_name).rlike(r"^\d{4}-\d{2}-\d{2}"),  "YYYY-MM-DD")
         .when(F.col(col_name).rlike(r"^\d{2}-\d{2}-\d{4}"),  "DD-MM-YYYY")
         .otherwise("UNKNOWN — investigate")
    )

for col in date_cols:
    print(f"\n  Column: {col}")
    df_orders.withColumn("fmt", classify_date(col)) \
             .groupBy("_source_file_name", "fmt") \
             .agg(F.count("*").alias("count")) \
             .orderBy("_source_file_name", "fmt") \
             .show(truncate=False)


# COMMAND ----------

# DBTITLE 1,Order Date Sequence Validation: Detect Impossible Timestamp Sequences
# Validate business logic: approved date must be >= purchase date, delivered >= approved
# Any violations = corrupt data that Silver must quarantine

section("5.4  orders — are there logically impossible date sequences?")
# After Silver parses dates to timestamps, these sequences must hold:
#   purchase  ≤  approved  ≤  carrier_pickup  ≤  delivered_to_customer
# Violations = bad data that must be quarantined, not silently passed through.

orders_ts = df_orders \
    .withColumn("t_purchase",
        F.coalesce(
            F.try_to_timestamp("order_purchase_date", F.lit("MM/dd/yyyy HH:mm")),
            F.try_to_timestamp("order_purchase_date", F.lit("yyyy-MM-dd HH:mm:ss")),
            F.try_to_timestamp("order_purchase_date", F.lit("yyyy-MM-dd H:mm")),
        )
    ).withColumn("t_approved",
        F.coalesce(
            F.try_to_timestamp("order_approved_at", F.lit("MM/dd/yyyy HH:mm")),
            F.try_to_timestamp("order_approved_at", F.lit("yyyy-MM-dd HH:mm:ss")),
            F.try_to_timestamp("order_approved_at", F.lit("yyyy-MM-dd H:mm")),
        )
    ).withColumn("t_delivered",
        F.coalesce(
            F.try_to_timestamp("order_delivered_customer_date", F.lit("MM/dd/yyyy HH:mm")),
            F.try_to_timestamp("order_delivered_customer_date", F.lit("yyyy-MM-dd HH:mm:ss")),
            F.try_to_timestamp("order_delivered_customer_date", F.lit("yyyy-MM-dd H:mm")),
        )
    )

checks = {
    "approved BEFORE purchase"  : (F.col("t_approved")  < F.col("t_purchase")),
    "delivered BEFORE approved" : (F.col("t_delivered") < F.col("t_approved")),
    "purchase date in future"   : (F.col("t_purchase")  > F.current_timestamp()),
}

for label, condition in checks.items():
    cnt = orders_ts.where(
        F.col("t_purchase").isNotNull() &
        F.col("t_approved").isNotNull() &
        condition
    ).count()
    print(f"  {label}: {cnt:,} rows")

# COMMAND ----------

# DBTITLE 1,Transaction Numeric Column Validation: Type Casting and Range Checks
# Validate numeric columns in transactions: Sales, Quantity, discount
# Test if they can be cast to proper numeric types and check for out-of-range values

section("5.5  transactions — what are the actual data types of Sales, Quantity, discount?")
# Spark may have inferred STRING for numeric columns due to mixed formats.
# We test castability: if cast fails → non-numeric junk in source.
# We also find the value distribution to catch out-of-range values.

print("  Sales — castability and range:")
df_transactions.select(
    F.expr("try_cast(Sales as DOUBLE)").alias("Sales_num")
).agg(
    F.count("*").alias("total"),
    F.count(F.when(F.col("Sales_num").isNull(), 1)).alias("non_numeric_Sales"),
    F.min("Sales_num").alias("min_Sales"),
    F.max("Sales_num").alias("max_Sales"),
    F.count(F.when(F.col("Sales_num") <= 0, 1)).alias("zero_or_negative_Sales"),
).show(truncate=False)

print("  Quantity — castability and range:")
df_transactions.select(
    F.expr("try_cast(Quantity as INT)").alias("Quantity_num")
).agg(
    F.count("*").alias("total"),
    F.count(F.when(F.col("Quantity_num").isNull(), 1)).alias("non_numeric_Quantity"),
    F.min("Quantity_num").alias("min_Qty"),
    F.max("Quantity_num").alias("max_Qty"),
    F.count(F.when(F.col("Quantity_num") <= 0, 1)).alias("zero_or_negative_Qty"),
).show(truncate=False)

print("  discount — format variants per file (looking for '%' string format):")
df_transactions \
    .withColumn("discount_format",
        F.when(F.col("discount").isNull(),                          "NULL")
         .when(F.col("discount").rlike(r"%$"),                      "PERCENT_STRING  e.g. 40%")
         .when(F.expr("try_cast(discount as DOUBLE)").isNotNull(),   "DECIMAL_STRING  e.g. 0.4")
         .otherwise("OTHER — investigate")
    ) \
    .groupBy("_source_file_name", "discount_format") \
    .agg(F.count("*").alias("count")) \
    .orderBy("_source_file_name") \
    .show(truncate=False)

print("  discount — are any decimal values > 1.0 or < 0? (invalid range):")
df_transactions \
    .where(~F.col("discount").rlike(r"%$") & F.col("discount").isNotNull()) \
    .select(F.expr("try_cast(discount as DOUBLE)").alias("d")) \
    .agg(
        F.count(F.when(F.col("d") > 1.0, 1)).alias("discount_gt_1_INVALID"),
        F.count(F.when(F.col("d") < 0.0, 1)).alias("discount_negative_INVALID"),
        F.min("d").alias("min_discount"),
        F.max("d").alias("max_discount"),
    ).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Transaction Profit Analysis: Distinguish Legitimate Losses from Corrupt Data
# Analyze negative profit rows: are they legitimate business losses or data corruption?
# Cross-check: negative profit with high discount = valid loss; negative profit with zero discount = suspicious

section("5.6  transactions — profit: are negatives real or corrupt?")
# A negative profit can be legitimate (high discount, loss-leader sale).
# Or it can be a sign of corrupt data (wrong sign, data entry error).
# We cross-check: negative profit rows — do they have high discounts?
# If discount is high and profit is negative, it is a valid business loss.
# If discount is 0 and profit is negative, it is likely corrupt.

print("  Profit distribution per source file:")
df_transactions.groupBy("_source_file_name").agg(
    F.count("*").alias("total"),
    F.count(F.when(F.col("profit").isNull(),  1)).alias("null_profit"),
    F.count(F.when(F.col("profit") < 0,       1)).alias("negative_profit"),
    F.count(F.when(F.col("profit") == 0,      1)).alias("zero_profit"),
    F.count(F.when(F.col("profit") > 0,       1)).alias("positive_profit"),
    F.round(F.min("profit"), 2).alias("min"),
    F.round(F.max("profit"), 2).alias("max"),
).orderBy("_source_file_name").show(truncate=False)

print("  Negative profit rows — do they have discount > 0? (legitimacy check):")
df_transactions \
    .where(F.col("profit") < 0) \
    .withColumn("discount_d",
        F.regexp_replace(F.col("discount"), "%", "").cast(T.DoubleType())
    ) \
    .withColumn("discount_normalised",
        F.when(F.col("discount").rlike(r"%$"), F.col("discount_d") / 100)
         .otherwise(F.col("discount_d"))
    ) \
    .agg(
        F.count("*").alias("total_negative_profit_rows"),
        F.count(F.when(F.col("discount_normalised") > 0, 1)).alias("have_discount_EXPECTED"),
        F.count(F.when(F.col("discount_normalised") == 0, 1)).alias("no_discount_SUSPICIOUS"),
    ).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Returns Date Format Detection: Including Literal String 'NULL' Bug
# Classify all return date formats, including the bug where 'NULL' is stored as a string literal
# Regex-based discovery instead of assuming known formats

section("5.7  returns — date_of_return: all format variants including string 'NULL'")
# We do not know the formats in advance. Regex-classify every value.
# Any value that does not match a known date pattern is a problem for Silver.

df_returns \
    .withColumn("date_fmt",
        F.when(F.col("date_of_return").isNull(),                        "IS_SQL_NULL")
         .when(F.upper(F.trim(F.col("date_of_return"))) == "NULL",      "STRING_NULL  ◄ bug in source")
         .when(F.col("date_of_return").rlike(r"^\d{4}-\d{2}-\d{2}$"),  "YYYY-MM-DD")
         .when(F.col("date_of_return").rlike(r"^\d{2}-\d{2}-\d{4}$"),  "DD-MM-YYYY")
         .when(F.col("date_of_return").rlike(r"^\d{2}/\d{2}/\d{4}$"),  "MM/DD/YYYY")
         .otherwise("UNKNOWN — investigate")
    ) \
    .groupBy("date_fmt") \
    .agg(
        F.count("*").alias("count"),
        F.slice(F.collect_list("date_of_return"), 1, 3).alias("samples")
    ) \
    .show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Returns NaN vs NULL Detection: Measure NaN Prevalence in Amount Columns
# Measure NaN vs NULL in numeric amount columns
# NaN propagates through aggregations; NULL is ignored. Silver must convert NaN → NULL.

section("5.8  returns — NaN vs NULL in numeric amount columns")
# JSON NaN is NOT SQL NULL. In Spark, NaN propagates through aggregations
# (avg of [1, NaN, 3] = NaN, not 2). NULL is ignored by aggregations.
# Silver must explicitly convert NaN → NULL using nanvl() or isnan().
# This cell measures how many NaN values exist so we know the scope of the problem.

df_returns.agg(
    F.count("*").alias("total_rows"),
    # refund_amount
    F.count(F.when(F.col("refund_amount").isNull(),      1)).alias("refund_NULL"),
    F.count(F.when(F.isnan("refund_amount"),             1)).alias("refund_NaN"),
    F.count(F.when(
        F.col("refund_amount").isNotNull() & ~F.isnan("refund_amount"), 1
    )).alias("refund_valid_number"),
    # amount
    F.count(F.when(F.col("amount").isNull(),             1)).alias("amount_NULL"),
    F.count(F.when(F.isnan("amount"),                    1)).alias("amount_NaN"),
    F.count(F.when(
        F.col("amount").isNotNull() & ~F.isnan("amount"), 1
    )).alias("amount_valid_number"),
).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Returns Reason and Status Values: Identify Garbage Values and Casing Variants
# Collect all distinct return reason and status values
# Short or symbol values ("?", "N/A", "-") are garbage that Silver must convert to null

section("5.9  returns — all distinct reason and status values (garbage detection)")
# We collect every distinct value. Short or symbol values ("?", "N/A", "-")
# are garbage that Silver must convert to null. Casing variants must be normalised.

print("  'reason' distinct values:")
df_returns.where(F.col("reason").isNotNull()) \
    .groupBy("reason").agg(F.count("*").alias("count")) \
    .orderBy(F.desc("count")).show(50, truncate=False)

print("  'return_reason' distinct values:")
df_returns.where(F.col("return_reason").isNotNull()) \
    .groupBy("return_reason").agg(F.count("*").alias("count")) \
    .orderBy(F.desc("count")).show(50, truncate=False)

print("  'status' distinct values:")
df_returns.where(F.col("status").isNotNull()) \
    .groupBy("status").agg(F.count("*").alias("count")) \
    .orderBy(F.desc("count")).show(50, truncate=False)

print("  'return_status' distinct values:")
df_returns.where(F.col("return_status").isNotNull()) \
    .groupBy("return_status").agg(F.count("*").alias("count")) \
    .orderBy(F.desc("count")).show(50, truncate=False)

# COMMAND ----------

# DBTITLE 1,Order Status and Ship Mode: Casing and Label Variants Per File
# Analyze ship_mode and order_status values per file to find casing/labeling inconsistencies
# Silver needs normalization rules to unify these variants

section("5.10  orders — ship_mode and order_status: casing and label variants")

print("  ship_mode distinct values per file:")
df_orders.groupBy("_source_file_name", "ship_mode") \
    .agg(F.count("*").alias("count")) \
    .orderBy("_source_file_name", F.desc("count")) \
    .show(truncate=False)

print("  order_status distinct values per file:")
df_orders.groupBy("_source_file_name", "order_status") \
    .agg(F.count("*").alias("count")) \
    .orderBy("_source_file_name", F.desc("count")) \
    .show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Product UPC Format and ID Prefix Analysis
# Analyze UPC format variations and product_id prefix distribution
# UPC may have scientific notation or proper numeric format; prefixes should match categories

section("5.11  products — UPC format and product_id prefix distribution")
# UPC landed as string. What formats are present?
# product_id has a prefix (FUR, OFF, TEC). Do those prefixes match the
# category? Mismatches = catalog data quality issue.

print("  UPC format breakdown:")
df_products \
    .withColumn("upc_fmt",
        F.when(F.col("upc").isNull(),                         "NULL")
         .when(F.col("upc").rlike(r"^\d+\.\d+E[+\-]\d+$"),  "SCIENTIFIC_NOTATION")
         .when(F.col("upc").rlike(r"^\d{10,13}$"),           "PROPER_NUMERIC")
         .otherwise("OTHER")
    ) \
    .groupBy("upc_fmt").agg(F.count("*").alias("count")).show(truncate=False)

print("  product_id prefix distribution:")
df_products \
    .withColumn("prefix", F.substring(F.col("product_id"), 1, 3)) \
    .groupBy("prefix").agg(F.count("*").alias("count")) \
    .orderBy(F.desc("count")).show(truncate=False)

print("  Do prefixes match categories? (FUR=Furniture, OFF=Office, TEC=Technology)")
df_products \
    .withColumn("prefix", F.substring(F.col("product_id"), 1, 3)) \
    .groupBy("prefix", "category").agg(F.count("*").alias("count")) \
    .orderBy("prefix", F.desc("count")).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Cross-Region Orders: Same Order ID in Multiple Regions (Duplication)
# Detect orders that appear in more than one region
# This is a cross-regional duplication issue that Silver must deduplicate

section("6.1  orders — order_id appearing in more than one region")

df_orders.groupBy("order_id") \
    .agg(
        F.countDistinct("source_region").alias("region_count"),
        F.count("*").alias("total_rows"),
        F.collect_set("source_region").alias("regions"),
    ) \
    .where(F.col("region_count") > 1) \
    .orderBy(F.desc("total_rows")) \
    .show(30, truncate=False)

# COMMAND ----------

# DBTITLE 1,Cross-Region Transactions: Same Order ID Transacted in Multiple Regions
# Detect transactions where the same order_id appears in multiple regions
# Must recover Order_ID from _rescued_data for R3 first

section("6.2  transactions — same order_id transacted in multiple regions")
# For Region 3, Order_ID must be recovered from _rescued_data first.

txn_with_id = df_transactions.withColumn(
    "canonical_order_id",
    F.when(
        F.col("Order_id").isNull() & F.col("_rescued_data").isNotNull(),
        F.get_json_object("_rescued_data", "$.Order_ID")
    ).otherwise(F.coalesce("Order_id", "Order_ID"))
)

txn_with_id.groupBy("canonical_order_id") \
    .agg(
        F.countDistinct("source_region").alias("region_count"),
        F.count("*").alias("total_rows"),
        F.collect_set("source_region").alias("regions"),
    ) \
    .where(F.col("region_count") > 1) \
    .orderBy(F.desc("total_rows")) \
    .show(30, truncate=False)

# COMMAND ----------

# DBTITLE 1,Cross-Region Customers: Same Email in Multiple Regions (Master Data Issue)
# Detect customers where the same email appears in multiple regions
# This indicates the same customer registered in multiple regional systems

section("6.3  customers — same email appearing in multiple regions")

df_customers.select(
    F.lower(F.trim(
        F.coalesce("customer_email", "email_address")
    )).alias("email_norm"),
    F.coalesce("customer_id","CustomerID","cust_id","customer_identifier").alias("cid"),
    "source_region"
).where(F.col("email_norm").isNotNull()) \
 .groupBy("email_norm") \
 .agg(
     F.countDistinct("source_region").alias("region_count"),
     F.countDistinct("cid").alias("id_count"),
     F.collect_set("source_region").alias("regions"),
 ) \
 .where(F.col("region_count") > 1) \
 .orderBy(F.desc("region_count")) \
 .show(30, truncate=False)

# COMMAND ----------

# DBTITLE 1,Multiple Returns for Same Order: Fraud Signal Detection
# Detect orders that have been returned more than once
# Multiple returns for the same order = potential fraud signal

section("6.4  returns — same order_id returned more than once (fraud signal)")

df_returns.withColumn(
    "coid", F.coalesce("order_id", "OrderId")
).groupBy("coid") \
 .agg(
     F.count("*").alias("return_count"),
     F.collect_set("_source_file_name").alias("files"),
 ) \
 .where(F.col("return_count") > 1) \
 .orderBy(F.desc("return_count")) \
 .show(20, truncate=False)

# COMMAND ----------

# DBTITLE 1,Product Duplicates: Check for Duplicate Product IDs
# Check if any product_id appears more than once in the product catalog
# Duplicates = master data quality issue that Silver must deduplicate

section("6.5  products — duplicate product_id check")

df_products.groupBy("product_id") \
    .agg(F.count("*").alias("count")) \
    .where(F.col("count") > 1) \
    .orderBy(F.desc("count")) \
    .show(20, truncate=False)

# COMMAND ----------

# DBTITLE 1,Build Canonical ID Sets for Referential Integrity Checks
# Build distinct ID sets from master/reference tables to use for join checks
# These are the "valid" IDs that transactional tables should reference

section("7.1  Build canonical ID sets for join checks")

canonical_customer_ids = df_customers.select(
    F.coalesce("customer_id","CustomerID","cust_id","customer_identifier")
     .alias("cid")
).distinct()

canonical_order_ids = df_orders.select("order_id").distinct()

vendor_ids = df_vendors.select("vendor_id").distinct()

# COMMAND ----------

# DBTITLE 1,Orphaned Orders: Orders with Invalid Customer IDs
# Detect orders that reference customer_ids that don't exist in the customers table
# Orphaned orders must be quarantined in Silver

section("7.2  orders — customer_id with no matching customer (orphaned orders)")

orphaned_orders = df_orders.join(
    canonical_customer_ids,
    df_orders.customer_id == canonical_customer_ids.cid,
    "left_anti"
)
print(f"  Orphaned orders (no customer match): {orphaned_orders.count():,}")
orphaned_orders.select("order_id", "customer_id", "_source_file_name") \
    .show(20, truncate=False)

# COMMAND ----------

# DBTITLE 1,Orders with Invalid Vendor IDs: Referential Integrity to Vendors
# Detect orders that reference vendor_ids that don't exist in the vendors table
# These are data quality warnings (vendor_id can be null in Silver)

section("7.3  orders — vendor_id with no matching vendor")

orphaned_vendor = df_orders.join(
    vendor_ids,
    df_orders.vendor_id == vendor_ids.vendor_id,
    "left_anti"
)
print(f"  Orders with invalid vendor_id: {orphaned_vendor.count():,}")
orphaned_vendor.select("order_id", "vendor_id", "_source_file_name") \
    .show(20, truncate=False)

# COMMAND ----------

# DBTITLE 1,Orphaned Transactions: Transactions with No Matching Order
# Detect transactions that reference order_ids that don't exist in the orders table
# These must be quarantined — a transaction cannot exist without a parent order

section("7.4  transactions — order_id with no matching order")

txn_orphaned = txn_with_id.join(
    canonical_order_ids,
    txn_with_id.canonical_order_id == canonical_order_ids.order_id,
    "left_anti"
)
print(f"  Orphaned transactions (no order match): {txn_orphaned.count():,}")
txn_orphaned.select("canonical_order_id", "Product_id", "_source_file_name") \
    .show(20, truncate=False)

# COMMAND ----------

# DBTITLE 1,Orphaned Returns: Returns with No Matching Order (Highest Fraud Risk)
# Detect returns that reference order_ids that don't exist in the orders table
# This is the highest fraud risk — returns without valid orders must be quarantined

section("7.5  returns — order_id with no matching order (highest fraud risk)")

ret_with_id = df_returns.withColumn(
    "coid", F.coalesce("order_id", "OrderId")
)
ret_orphaned = ret_with_id.join(
    canonical_order_ids,
    ret_with_id.coid == canonical_order_ids.order_id,
    "left_anti"
)
print(f"  Orphaned returns (no order match): {ret_orphaned.count():,}")
ret_orphaned.select("coid", "_source_file_name") \
    .show(20, truncate=False)

# COMMAND ----------

# DBTITLE 1,Silver Readiness Checklist: Define Check Function and Row Counts
# Define helper function for readiness checks and compute total row counts
# This sets up the framework for evaluating Silver transformation requirements

section("8 — Silver Readiness Checklist")

def check(label, fail_count, total, consequence="quarantine"):
    """Print a pass/fail status for a data quality check."""
    status = "✓ PASS" if fail_count == 0 else "✗ NEEDS FIX"
    pct    = round(fail_count / total * 100, 2) if total > 0 else 0
    print(f"  {status}  [{label}]  {fail_count:,} / {total:,} rows ({pct}%)  → {consequence}")

total_orders = df_orders.count()
total_txn    = df_transactions.count()
total_cust   = df_customers.count()
total_ret    = df_returns.count()
total_prod   = df_products.count()

# COMMAND ----------

# DBTITLE 1,Customers Readiness: ID, Email, and City/State Swap Checks
# Run data quality checks for customers table
# Verify that canonical IDs can be resolved, emails are present (with R5 exception), and city/state aren't swapped

print("\n  ── CUSTOMERS ──")
check("canonical_id resolvable",
    df_customers.where(
        F.col("customer_id").isNull() & F.col("CustomerID").isNull() &
        F.col("cust_id").isNull() & F.col("customer_identifier").isNull()
    ).count(), total_cust, "quarantine — no usable ID")

check("email present (warn only if R5)",
    df_customers.where(
        F.col("customer_email").isNull() & F.col("email_address").isNull()
    ).count(), total_cust, "warn + null accepted for R5")

check("city not a state name (R4 swap check)",
    df_customers.where(F.col("city").isin(known_states)).count(),
    total_cust, "swap city/state in Silver for R4")

# COMMAND ----------

# DBTITLE 1,Orders Readiness: ID, Date Parsing, and Logical Sequence Checks
# Run data quality checks for orders table
# Verify order_id and customer_id are not null, dates can be parsed, and date sequences are logical

print("\n  ── ORDERS ──")
check("order_id not null",
    df_orders.where(F.col("order_id").isNull()).count(),
    total_orders, "quarantine")

check("customer_id not null",
    df_orders.where(F.col("customer_id").isNull()).count(),
    total_orders, "quarantine")

check("purchase_date parseable",
    orders_ts.where(F.col("t_purchase").isNull()).count(),
    total_orders, "quarantine — cannot be used without date")

check("approved not before purchase",
    orders_ts.where(
        F.col("t_purchase").isNotNull() & F.col("t_approved").isNotNull() &
        (F.col("t_approved") < F.col("t_purchase"))
    ).count(), total_orders, "quarantine — impossible sequence")

# COMMAND ----------

# DBTITLE 1,Transactions Readiness: ID Recovery, Type Casting, and Discount Range Checks
# Run data quality checks for transactions table
# Verify order_id can be resolved (including rescued data), Sales/discount can be cast to numeric, and discount is in valid range

print("\n  ── TRANSACTIONS ──")
check("order_id resolvable (incl. rescued)",
    txn_with_id.where(F.col("canonical_order_id").isNull()).count(),
    total_txn, "quarantine — cannot link to order")

check("Sales castable to DOUBLE",
    df_transactions.select(
        F.expr("try_cast(Sales as DOUBLE)").alias("Sales_num")
    ).where(F.col("Sales_num").isNull()).count(), 
    total_txn, "quarantine — revenue figure unreadable")

check("discount in valid range after normalisation",
    df_transactions.withColumn("d_norm",
        F.when(F.col("discount").rlike(r"%$"),
            F.expr("try_cast(regexp_replace(discount, '%', '') as DOUBLE)") / 100
        ).otherwise(F.expr("try_cast(discount as DOUBLE)"))
    ).where(
        F.col("d_norm").isNotNull() & ((F.col("d_norm") > 1.0) | (F.col("d_norm") < 0.0))
    ).count(), total_txn, "quarantine — discount outside 0–1 range")

check("negative profit with discount > 0 (valid losses)",
    df_transactions.where(F.col("profit") < 0).count(),
    total_txn, "WARN only — log in quality_issues table, keep in Silver")

# COMMAND ----------

# DBTITLE 1,Returns Readiness: ID, Date, Reason, and NaN Checks
# Run data quality checks for returns table
# Verify order_id can be resolved, date_of_return is not literal 'NULL', reason is not garbage, and NaN is converted to null

print("\n  ── RETURNS ──")
check("order_id resolvable",
    df_returns.where(
        F.col("order_id").isNull() & F.col("OrderId").isNull()
    ).count(), total_ret, "quarantine")

check("date_of_return not a literal 'NULL' string",
    df_returns.where(
        F.upper(F.trim(F.col("date_of_return"))) == "NULL"
    ).count(), total_ret, "convert to SQL null in Silver")

check("reason not a garbage value",
    df_returns.where(
        F.col("reason").isin("?", "N/A", "-", "na", "none", "None")
    ).count(), total_ret, "convert to null in Silver")

check("refund_amount NaN converted",
    df_returns.where(F.isnan("refund_amount")).count(),
    total_ret, "nanvl(refund_amount, null) in Silver")

# COMMAND ----------

# DBTITLE 1,Products, Vendors, and Referential Integrity Final Checks
# Final data quality checks for products, vendors, and cross-table referential integrity
# Verify product_id and vendor_id are not null, products are unique, and all foreign keys are valid

print("\n  ── PRODUCTS ──")
check("product_id not null",
    df_products.where(F.col("product_id").isNull()).count(),
    total_prod, "quarantine")

check("product_id unique",
    df_products.count() - df_products.select("product_id").distinct().count(),
    total_prod, "deduplicate in Silver")

print("\n  ── VENDORS ──")
check("vendor_id not null",
    df_vendors.where(F.col("vendor_id").isNull()).count(),
    df_vendors.count(), "quarantine")

print("\n  ── REFERENTIAL INTEGRITY ──")
check("orders → customers",      orphaned_orders.count(),  total_orders, "quarantine orphaned orders")
check("orders → vendors",        orphaned_vendor.count(),  total_orders, "warn + null vendor_id in Silver")
check("transactions → orders",   txn_orphaned.count(),     total_txn,    "quarantine — no parent order")
check("returns → orders",        ret_orphaned.count(),     total_ret,    "quarantine — highest fraud risk")
