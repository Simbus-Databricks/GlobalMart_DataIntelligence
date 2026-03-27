# Databricks notebook source
# MAGIC %md
# MAGIC #Bronze_ingestion_pipeline
# MAGIC The Bronze layer serves as the raw ingestion layer in the data pipeline. It is responsible for capturing data from source systems in its original format with minimal or no transformations, ensuring data fidelity and traceability.

# COMMAND ----------

# DBTITLE 1,Imports for Spark Pipelines and SQL Functions
# Imports for Spark Pipelines and SQL Functions
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------

# DBTITLE 1,Fixed Base Paths and Bronze Table Settings
# Base paths — all paths derive from these two roots
 
VOLUME_BASE = (
    "/Volumes/globalmart_dev/default/globalmart_retail_data"
    "/GlobalMart_Retail_Data"
)
 
# Schema location kept separate from DLT storage so resetting pipeline
# state during development does not wipe Auto Loader schema history
SCHEMA_BASE = (
    "/Volumes/globalmart_dev/default/globalmart_retail_data"
    "/_autoloader_schema"
)
 
# Explicit region mapping for root-level files that have no Region sub-folder.
# Regional files get region extracted from path directly.
ROOT_FILE_REGION_MAP = {
    "transactions_3.csv": "Region 3",
    "returns_2.json":     "root",
    "products.json":      "global",
    "vendors.csv":        "global",
}
 
# Applied to all Bronze tables — Column Mapping enabled so source files
# with spaces or special characters in column names never fail at write time
BRONZE_TABLE_PROPERTIES = {
    "quality": "bronze",
    "pipelines.autoOptimize.managed": "true",
    "delta.enableChangeDataFeed": "true",
    "delta.columnMapping.mode": "name",
    "delta.minReaderVersion": "2",
    "delta.minWriterVersion": "5",
}

# COMMAND ----------

# DBTITLE 1,Extract Source Region from File Path with Fallback Logic
# Extract Source Region from File Path with Fallback Logic
def add_source_region(df):
    """
    Derive source_region from _source_file_path.
    Decodes %20 in path before regex so 'Region%204' matches as 'Region 4'.
    Falls back to ROOT_FILE_REGION_MAP for root-level files.
    Falls back to 'unknown' if neither matches.
    """
 
    decoded_path = F.regexp_replace(F.col("_source_file_path"), "%20", " ")
 
    fallback = F.lit("unknown")
    for filename, region in reversed(list(ROOT_FILE_REGION_MAP.items())):
        fallback = F.when(
            F.col("_source_file_name") == filename,
            F.lit(region),
        ).otherwise(fallback)
 
    source_region_col = F.when(
        F.regexp_extract(decoded_path, r"(Region \d+)", 1) != "",
        F.regexp_extract(decoded_path, r"(Region \d+)", 1)
    ).otherwise(fallback)
 
    return df.withColumn("source_region", source_region_col)

# COMMAND ----------

# DBTITLE 1,Build Streaming DataFrame for Bronze Ingestion
# Build Streaming DataFrame for Bronze Ingestion
def build_bronze_stream(
    format: str,
    glob_filter: str,
    schema_path: str,
    multiline: bool = False,
    schema_hints: str = "",
):
    """
    Build a streaming DataFrame using Auto Loader for Bronze ingestion.
 
    Parameters
    ----------
    format      : "csv" or "json"
    glob_filter : restricts which files this stream picks up
    schema_path : where Auto Loader persists inferred schema across runs
    multiline   : True for JSON files where records span multiple lines
    schema_hints: overrides inferred type for specific columns
 
    Returns
    -------
    Streaming DataFrame with source columns plus:
        source_region, _source_file_path, _source_file_name,
        _source_modified_time, _ingested_at
    """
 
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", format)
        .option("cloudFiles.schemaLocation", schema_path)
 
        # Detect proper types from data values instead of defaulting to string
        .option("cloudFiles.inferColumnTypes", "true")
 
        # Unknown columns land in _rescued_data — stream never stops on
        # schema change, no records dropped
        .option("cloudFiles.schemaEvolutionMode", "rescue")
    )
 
    if schema_hints:
        reader = reader.option("cloudFiles.schemaHints", schema_hints)
 
    raw_df = (
        reader
        # Scan VOLUME_BASE and all sub-folders so one stream covers both
        # root-level files and Region sub-folder files for the same entity
        .option("recursiveFileLookup", "true")
 
        # Only pick up files matching this pattern — prevents cross-entity
        # contamination across shared Region sub-folders
        .option("pathGlobFilter", glob_filter)
 
        .option("header", "true")
        .option("multiLine", str(multiline).lower())
        .load(VOLUME_BASE)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file_path"),
            F.col("_metadata.file_name").alias("_source_file_name"),
            F.col("_metadata.file_modification_time").alias("_source_modified_time"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )
 
    return add_source_region(raw_df)

# COMMAND ----------

# DBTITLE 1,Define Bronze Customer Records Streaming Data Ingestion
# Define Bronze Customer Records Streaming Data Ingestion
@dp.table(
    name="customers",
    comment="Bronze: raw customer records from all 6 regional systems. Schema variants preserved across regions — normalisation handled in Silver.",
    table_properties=BRONZE_TABLE_PROPERTIES,
)
def bronze_customers():
    return build_bronze_stream(
        format="csv",
        glob_filter="customers_*.csv",
        schema_path=f"{SCHEMA_BASE}/customers",
        schema_hints="postal_code STRING",
    )

# COMMAND ----------

# DBTITLE 1,Create Bronze Orders Table with CSV Ingestion and Schema
# Create Bronze Orders Table with CSV Ingestion and Schema
@dp.table(
    name="orders",
    comment="Bronze: raw order records from R1, R3, R5. Date formats and column differences preserved as received — standardised in Silver.",
    table_properties=BRONZE_TABLE_PROPERTIES,
)
def bronze_orders():
    return build_bronze_stream(
        format="csv",
        glob_filter="orders_*.csv",
        schema_path=f"{SCHEMA_BASE}/orders",
        schema_hints=(
            "order_purchase_date STRING, order_approved_at STRING, "
            "order_delivered_carrier_date STRING, "
            "order_delivered_customer_date STRING, "
            "order_estimated_delivery_date STRING"
        ),
    )

# COMMAND ----------

# DBTITLE 1,Define Bronze Transactions Streaming Data Ingestion
# Define Bronze Transactions Streaming Data Ingestion
@dp.table(
    name="transactions",
    comment="Bronze: raw transaction records from R2, R4, and volume root. Column casing and discount format differences preserved — normalised in Silver.",
    table_properties=BRONZE_TABLE_PROPERTIES,
)
def bronze_transactions():
    return build_bronze_stream(
        format="csv",
        glob_filter="transactions_*.csv",
        schema_path=f"{SCHEMA_BASE}/transactions",
        schema_hints="profit DOUBLE",
    )

# COMMAND ----------

# DBTITLE 1,Define Bronze Returns Table with JSON Ingestion and Schema
# Define Bronze Returns Table with JSON Ingestion and Schema
@dp.table(
    name="returns",
    comment="Bronze: raw return records from R6 and volume root. All column names differ between the two files — both sets preserved, coalesced in Silver.",
    table_properties=BRONZE_TABLE_PROPERTIES,
)
def bronze_returns():
    return build_bronze_stream(
        format="json",
        glob_filter="returns_*.json",
        schema_path=f"{SCHEMA_BASE}/returns",
        multiline=True,
        schema_hints="refund_amount DOUBLE, amount DOUBLE",
    )

# COMMAND ----------

# DBTITLE 1,Define Bronze Products Table for JSON Ingestion and Schema
# Define Bronze Products Table for JSON Ingestion and Schema
@dp.table(
    name="products",
    comment="Bronze: raw product catalogue from products.json. Single global reference file — no cross-region variation.",
    table_properties=BRONZE_TABLE_PROPERTIES,
)
def bronze_products():
    return build_bronze_stream(
        format="json",
        glob_filter="products.json",
        schema_path=f"{SCHEMA_BASE}/products",
        multiline=True,
        schema_hints="dateAdded STRING, dateUpdated STRING",
    )
    

# COMMAND ----------

# DBTITLE 1,Create Bronze Vendors Table with CSV Ingestion and Schema
# Create Bronze Vendors Table with CSV Ingestion and Schema
@dp.table(
    name="vendors",
    comment="Bronze: raw vendor reference data from vendors.csv. Single global reference file, 4 rows.",
    table_properties=BRONZE_TABLE_PROPERTIES,
)
def bronze_vendors():
    return build_bronze_stream(
        format="csv",
        glob_filter="vendors.csv",
        schema_path=f"{SCHEMA_BASE}/vendors",
    )
 