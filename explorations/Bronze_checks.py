# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
spark.sql("USE CATALOG `globalmart_dev`")
spark.sql("USE SCHEMA `bronze`")

# COMMAND ----------

# Get all tables in the schema
tables = spark.catalog.listTables("bronze")

# Loop and drop each table
for table in tables:
    spark.sql(f"DROP TABLE IF EXISTS bronze.{table.name}")

print("All tables dropped from bronze schema.")

# COMMAND ----------

# DBTITLE 1,Cell 2
import json

tables = spark.sql("""
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'bronze'
  AND table_catalog = 'globalmart_dev'
""").collect()

results = []

for t in tables:
    table_info = {
        "table_name": t.table_name,
        "sample_rows": [],
        "rescued_data": []
    }
    
    df = spark.read.table(f"globalmart_dev.bronze.{t.table_name}")
    
    # Get sample rows
    sample_rows = df.limit(5).toPandas()
    table_info["sample_rows"] = sample_rows.to_dict(orient='records')
    
    # Check for rescued data
    if '_rescued_data' in df.columns:
        rescued_rows = df.select('_rescued_data').where(df['_rescued_data'].isNotNull()).limit(5).toPandas()
        table_info["rescued_data"] = rescued_rows.to_dict(orient='records')
    
    results.append(table_info)

print(json.dumps(results, indent=2, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC #POST-RUN VERIFICATION QUERIES

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Row counts per table ---
# MAGIC SELECT 'customers'    AS entity, COUNT(*) AS rows FROM globalmart_dev.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT 'orders'       AS entity, COUNT(*) AS rows FROM globalmart_dev.bronze.orders
# MAGIC UNION ALL
# MAGIC SELECT 'transactions' AS entity, COUNT(*) AS rows FROM globalmart_dev.bronze.transactions
# MAGIC UNION ALL
# MAGIC SELECT 'returns'      AS entity, COUNT(*) AS rows FROM globalmart_dev.bronze.returns
# MAGIC UNION ALL
# MAGIC SELECT 'products'     AS entity, COUNT(*) AS rows FROM globalmart_dev.bronze.products
# MAGIC UNION ALL
# MAGIC SELECT 'vendors'      AS entity, COUNT(*) AS rows FROM globalmart_dev.bronze.vendors

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Records per source file (confirms all files were loaded)
# MAGIC SELECT _source_file_name, source_region, COUNT(*) AS rows
# MAGIC FROM globalmart_dev.bronze.customers
# MAGIC GROUP BY _source_file_name, source_region
# MAGIC ORDER BY source_region;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check _rescued_data for unexpected columns or type mismatches
# MAGIC SELECT _source_file_name, source_region, _rescued_data
# MAGIC FROM globalmart_dev.bronze.transactions
# MAGIC WHERE _rescued_data IS NOT NULL
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm source_region derivation for root-level files
# MAGIC SELECT _source_file_name, source_region
# MAGIC FROM globalmart_dev.bronze.transactions
# MAGIC GROUP BY _source_file_name, source_region
# MAGIC ORDER BY _source_file_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Idempotency Check
# MAGIC - Re-trigger the pipeline with no new files
# MAGIC - Verify:
# MAGIC   - No new records are added
# MAGIC   - Row counts remain unchanged
# MAGIC   - DLT event log shows 0 inserts
# MAGIC
# MAGIC ### Expected Source Region Mapping
# MAGIC - transactions_3.csv → Region 3
# MAGIC - transactions_1.csv → Region 2
# MAGIC - transactions_2.csv → Region 4
# MAGIC
# MAGIC ### DLT Expectation Validation
# MAGIC - Open DLT Pipeline UI
# MAGIC - Select a table → Data Quality tab
# MAGIC - Check for expectation violations
# MAGIC - Any non-zero violations should be investigated in Silver layer

# COMMAND ----------

tables = spark.sql("""
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'bronze'
  AND table_catalog = 'globalmart_dev'
""").collect()

for t in tables:
    spark.sql(f"DROP TABLE IF EXISTS globalmart_dev.bronze.{t.table_name}")
