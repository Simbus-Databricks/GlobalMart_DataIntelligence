# Databricks notebook source
# MAGIC %md
# MAGIC ##CREATE CATALOG
# MAGIC
# MAGIC - globalmart_dev is the single catalog for the entire POC.
# MAGIC - Bronze, Silver, and Gold all live as schemas inside this catalog.
# MAGIC - Using one catalog keeps lineage graphs connected across all three layers — Unity Catalog can </br> 
# MAGIC trace a Gold metric back through Silver to Bronze to the aw Volume file without crossing catalog boundaries.
# MAGIC

# COMMAND ----------

spark.sql("""
    CREATE CATALOG IF NOT EXISTS globalmart_dev
    COMMENT 'GlobalMart Data Intelligence Platform — POC catalog.
             Contains Bronze (raw), Silver (cleansed), and Gold (star schema)
             schemas. All data originates from 6 regional retail systems.'
""")

print("Catalog globalmart_dev: ready")


# COMMAND ----------

# MAGIC %md
# MAGIC ##USE CATALOG
# MAGIC - Set the default catalog for all subsequent SQL statements in this session.
# MAGIC - Avoids having to fully qualify every object as globalmart_dev.<schema>.<table>

# COMMAND ----------

spark.sql("USE CATALOG globalmart_dev")
print("Active catalog: globalmart_dev")


# COMMAND ----------

# MAGIC %md
# MAGIC #CREATE BRONZE,SILVER AND GOLD SCHEMA
# MAGIC - Bronze schema holds raw ingestion tables only — one per entity.
# MAGIC

# COMMAND ----------

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS globalmart_dev.bronze
    COMMENT 'Bronze layer — raw ingestion from all 6 regional source systems.
             Tables: customers, orders, transactions, returns, products, vendors.
             Data is loaded exactly as received — no transformations applied.'
""")
 
print("Schema globalmart_dev.bronze: ready")
 
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS globalmart_dev.silver
    COMMENT 'Silver layer — cleansed and validated data from Bronze.
             Tables: customers, orders, transactions, returns, products, vendors.
             Data quality checks and standardization applied.'
""")
 
print("Schema globalmart_dev.silver: ready")
 
spark.sql("""
    CREATE SCHEMA IF NOT EXISTS globalmart_dev.gold
    COMMENT 'Gold layer — star schema for analytics and reporting.
             Tables: fact and dimension tables derived from Silver.
             Data is modeled for BI and GenAI use cases.'
""")
 
print("Schema globalmart_dev.gold: ready")
 

# COMMAND ----------

# MAGIC %md
# MAGIC #VOLUME SETUP & DATA LOAD
# MAGIC - This script sets up and populates a Unity Catalog volume for storing
# MAGIC - raw GlobalMart retail data used in the Bronze ingestion layer.
# MAGIC
# MAGIC ### Notes:
# MAGIC - Assumes the source folder already exists in DBFS.
# MAGIC - Uses `dbutils.fs.cp` for recursive copy.
# MAGIC - Intended for initial data setup before Bronze layer processing.
# MAGIC

# COMMAND ----------

VOLUME_PATH = "/Volumes/globalmart_dev/default/globalmart_retail_data"

# Create the volume if it does not exist
try:
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS globalmart_dev.default.globalmart_retail_data
        COMMENT 'Raw retail data files for Bronze ingestion'
    """)
    print("Volume created or already exists.")
except Exception as e:
    print(f"Error creating volume: {e}")

# Upload the folder to the volume (assumes local folder path is available)
local_folder_path = "/dbfs/tmp/GlobalMart_Retail_Data"  
dbutils.fs.cp(local_folder_path, f"{VOLUME_PATH}/GlobalMart_Retail_Data", recurse=True)
print("Folder uploaded to volume.")

files = dbutils.fs.ls(f"{VOLUME_PATH}/GlobalMart_Retail_Data")
print(f"Root-level entries in volume ({len(files)} items):")
for f in files:
    print(f"  {f.path}")

# Recurse into each Region sub-folder to confirm regional files
print("\nRegional files:")
for item in files:
    if "Region" in item.name:
        region_files = dbutils.fs.ls(item.path)
        for rf in region_files:
            print(f"  {rf.path}")


# COMMAND ----------

def walk_volume(path):
    """Recursively list all files under a volume path."""
    all_files = []
    items = dbutils.fs.ls(path)
    for item in items:
        if item.isDir():
            all_files.extend(walk_volume(item.path))
        else:
            all_files.append(item.path)
    return all_files

all_files = walk_volume(f"{VOLUME_PATH}/GlobalMart_Retail_Data")

# Group by entity
entity_counts = {
    "customers": [f for f in all_files if "customers_" in f],
    "orders":    [f for f in all_files if "orders_" in f],
    "transactions": [f for f in all_files if "transactions_" in f],
    "returns":   [f for f in all_files if "returns_" in f],
    "products":  [f for f in all_files if "products" in f],
    "vendors":   [f for f in all_files if "vendors" in f],
}

print(f"Total files found: {len(all_files)}\n")
for entity, paths in entity_counts.items():
    status = "OK" if len(paths) > 0 else "MISSING"
    print(f"  [{status}] {entity}: {len(paths)} file(s)")
    for p in paths:
        print(f"         {p}")

expected_total = 16
if len(all_files) == expected_total:
    print(f"\nAll {expected_total} source files confirmed. Safe to run 02_bronze_pipeline.py")
else:
    print(f"\nWARNING: Expected {expected_total} files, found {len(all_files)}.")
    print("Resolve missing files before running the Bronze pipeline.")


# COMMAND ----------

# MAGIC %md
# MAGIC # RBAC: ENGINEERING ACCESS ON BRONZE SCHEMA
# MAGIC - Engineering group needs full privileges on the bronze schema to:
# MAGIC    - Run and monitor the DLT pipeline
# MAGIC    - Query Bronze tables directly for debugging
# MAGIC    - Create and drop tables during pipeline development

# COMMAND ----------

# DBTITLE 1,Cell 12
# Create the engineering group if it doesn't exist
try:
    spark.sql("CREATE GROUP globalmart_engineering")
    print("Created group: globalmart_engineering")
except Exception as e:
    if "PRINCIPAL_ALREADY_EXISTS" in str(e):
        print("Group globalmart_engineering already exists")
    else:
        raise

spark.sql("""
    GRANT USE SCHEMA ON SCHEMA globalmart_dev.bronze
    TO `globalmart_engineering`
""")

spark.sql("""
    GRANT SELECT ON SCHEMA globalmart_dev.bronze
    TO `globalmart_engineering`
""")

spark.sql("""
    GRANT MODIFY ON SCHEMA globalmart_dev.bronze
    TO `globalmart_engineering`
""")

spark.sql("""
    GRANT CREATE TABLE ON SCHEMA globalmart_dev.bronze
    TO `globalmart_engineering`
""")

print("RBAC grants on globalmart_dev.bronze: applied for globalmart_engineering")


spark.sql("""
    GRANT READ VOLUME ON VOLUME globalmart_dev.default.globalmart_retail_data
    TO `globalmart_engineering`
""")

spark.sql("""
    GRANT WRITE VOLUME ON VOLUME globalmart_dev.default.globalmart_retail_data
    TO `globalmart_engineering`
""")

print("Volume grants: globalmart_engineering can read and write the raw data volume")


# COMMAND ----------

# MAGIC %md
# MAGIC #VERIFY SETUP BEFORE RUNNING PIPELINE
# MAGIC
# MAGIC - Final check: confirm catalog and schema exist and are queryable.
# MAGIC - Also lists any tables already in the bronze schema

# COMMAND ----------

# =============================================================================


print("=== SETUP VERIFICATION ===\n")

# Confirm catalog exists
catalogs = spark.sql("SHOW CATALOGS").collect()
catalog_names = [r[0] for r in catalogs]
print(f"Catalog globalmart_dev exists: {'globalmart_dev' in catalog_names}")

# Confirm bronze schema exists
schemas = spark.sql("SHOW SCHEMAS IN globalmart_dev").collect()
schema_names = [r[0] for r in schemas]
print(f"Schema bronze exists: {'bronze' in schema_names}")

# List tables in bronze (empty before pipeline runs)
tables = spark.sql("SHOW TABLES IN globalmart_dev.bronze").collect()
if tables:
    print(f"\nTables in globalmart_dev.bronze ({len(tables)}):")
    for t in tables:
        print(f"  {t.tableName}")
else:
    print("\nNo tables in globalmart_dev.bronze yet")

print("\nCatelog Setup complete")


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS SILVER