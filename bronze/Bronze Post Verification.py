# Databricks notebook source
# DBTITLE 1,Cell 1
# Set active catalog context for all subsequent queries
# USE: Ensures all queries run against the correct development catalog
# without needing to fully qualify every table reference

spark.sql("USE CATALOG globalmart_dev")
print("Active catalog: globalmart_dev")

# COMMAND ----------

# DBTITLE 1,Row counts per table
# MAGIC %sql
# MAGIC -- Check 1: Row Count Sanity Check
# MAGIC -- PURPOSE: Verify that data was successfully ingested into all bronze tables
# MAGIC -- USE: Quick smoke test to ensure pipeline ran and tables are not empty
# MAGIC -- EXPECTED: Non-zero counts for all 6 entities
# MAGIC     
# MAGIC SELECT 'customers'    AS entity, COUNT(*) AS rows FROM globalmart_dev.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT 'orders',       COUNT(*) FROM globalmart_dev.bronze.orders
# MAGIC UNION ALL
# MAGIC SELECT 'transactions', COUNT(*) FROM globalmart_dev.bronze.transactions
# MAGIC UNION ALL
# MAGIC SELECT 'returns',      COUNT(*) FROM globalmart_dev.bronze.returns
# MAGIC UNION ALL
# MAGIC SELECT 'products',     COUNT(*) FROM globalmart_dev.bronze.products
# MAGIC UNION ALL
# MAGIC SELECT 'vendors',      COUNT(*) FROM globalmart_dev.bronze.vendors

# COMMAND ----------

# DBTITLE 1,Records per source file (confirms all 16 files loaded)
# MAGIC %sql
# MAGIC -- Check 2: Source File Completeness (Customers)
# MAGIC -- PURPOSE: Verify Auto Loader ingested ALL source files (4 regions x 4 files = 16 total)
# MAGIC -- USE: Detects missing files, skipped regions, or incomplete ingestion
# MAGIC -- EXPECTED: 16 distinct file names with reasonable row distribution across all regions
# MAGIC     
# MAGIC SELECT
# MAGIC     _source_file_name,
# MAGIC     source_region,
# MAGIC     COUNT(*) AS rows
# MAGIC FROM globalmart_dev.bronze.customers
# MAGIC GROUP BY _source_file_name, source_region
# MAGIC ORDER BY source_region

# COMMAND ----------

# DBTITLE 1,ID column population per region (customers)
# MAGIC %sql
# MAGIC -- Check 3: Schema Variation Detection - ID Columns (Customers)
# MAGIC -- PURPOSE: Identify which ID column naming conventions exist per region
# MAGIC -- USE: Reveals schema heterogeneity from different source systems
# MAGIC -- EXPECTED: Each region populates ONE ID column variant; others are NULL
# MAGIC -- ACTION: Informs silver layer unification logic (COALESCE strategy)
# MAGIC     
# MAGIC SELECT
# MAGIC     source_region,
# MAGIC     COUNT(customer_id)          AS has_customer_id,
# MAGIC     COUNT(CustomerID)           AS has_CustomerID,
# MAGIC     COUNT(cust_id)              AS has_cust_id,
# MAGIC     COUNT(customer_identifier)  AS has_customer_identifier
# MAGIC FROM globalmart_dev.bronze.customers
# MAGIC GROUP BY source_region
# MAGIC ORDER BY source_region

# COMMAND ----------

# DBTITLE 1,mail column population per region (customers)
# MAGIC %sql
# MAGIC -- Check 4: Schema Variation Detection - Email Columns (Customers)
# MAGIC -- PURPOSE: Identify email column naming conventions per region
# MAGIC -- USE: Another example of regional schema heterogeneity
# MAGIC -- EXPECTED: Each region uses one of two email column variants
# MAGIC -- ACTION: Confirms need for COALESCE(customer_email, email_address) in silver
# MAGIC     
# MAGIC SELECT
# MAGIC     source_region,
# MAGIC     COUNT(customer_email)  AS has_customer_email,
# MAGIC     COUNT(email_address)   AS has_email_address
# MAGIC FROM globalmart_dev.bronze.customers
# MAGIC GROUP BY source_region
# MAGIC ORDER BY source_region

# COMMAND ----------

# DBTITLE 1,Rescued data inspection (customers)
# MAGIC %sql
# MAGIC -- Check 5: Rescued Data Inspection (Customers)
# MAGIC -- PURPOSE: Detect schema mismatches, unexpected fields, or malformed records
# MAGIC -- USE: Auto Loader's _rescued_data column captures fields that don't match schema
# MAGIC -- EXPECTED: Ideally NULL; non-NULL indicates schema drift or data quality issues
# MAGIC -- ACTION: If present, investigate source files and update schema inference or validation
# MAGIC     
# MAGIC SELECT
# MAGIC     _source_file_name,
# MAGIC     source_region,
# MAGIC     _rescued_data
# MAGIC FROM globalmart_dev.bronze.customers
# MAGIC WHERE _rescued_data IS NOT NULL
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Rescued data inspection (transactions)
# MAGIC %sql
# MAGIC -- Check 6: Rescued Data Inspection (Transactions)
# MAGIC -- PURPOSE: Detect schema mismatches for transactions entity
# MAGIC -- USE: Same as customers check - validates schema consistency
# MAGIC -- EXPECTED: NULL for all records if schema is stable
# MAGIC -- ACTION: Non-NULL values require investigation of transaction source files
# MAGIC     
# MAGIC SELECT
# MAGIC     _source_file_name,
# MAGIC     source_region,
# MAGIC     _rescued_data
# MAGIC FROM globalmart_dev.bronze.transactions
# MAGIC WHERE _rescued_data IS NOT NULL
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Transactions source file breakdown
# MAGIC %sql
# MAGIC -- Check 7: Source File Completeness (Transactions)
# MAGIC -- PURPOSE: Verify all transaction files loaded across all regions
# MAGIC -- USE: Parallel to customers file check - ensures no missing transaction data
# MAGIC -- EXPECTED: Multiple files per region with balanced row distribution
# MAGIC -- ACTION: Missing files indicate incomplete data pipeline execution
# MAGIC     
# MAGIC SELECT
# MAGIC     _source_file_name,
# MAGIC     source_region,
# MAGIC     COUNT(*) AS rows
# MAGIC FROM globalmart_dev.bronze.transactions
# MAGIC GROUP BY _source_file_name, source_region
# MAGIC ORDER BY source_region

# COMMAND ----------

# DBTITLE 1,Returns column structure
# MAGIC %sql
# MAGIC -- Check 8: Schema Variation Detection - Returns Columns
# MAGIC -- PURPOSE: Identify column naming variations in returns entity across regions
# MAGIC -- USE: Maps out schema heterogeneity for order_id, reason, amount, and status fields
# MAGIC -- EXPECTED: Each file/region uses one variant per field group
# MAGIC -- ACTION: Documents required COALESCE logic for silver layer standardization
# MAGIC     
# MAGIC SELECT
# MAGIC     _source_file_name,
# MAGIC     source_region,
# MAGIC     COUNT(order_id)       AS has_order_id,
# MAGIC     COUNT(OrderId)        AS has_OrderId,
# MAGIC     COUNT(return_reason)  AS has_return_reason,
# MAGIC     COUNT(reason)         AS has_reason,
# MAGIC     COUNT(refund_amount)  AS has_refund_amount,
# MAGIC     COUNT(amount)         AS has_amount,
# MAGIC     COUNT(return_status)  AS has_return_status,
# MAGIC     COUNT(status)         AS has_status
# MAGIC FROM globalmart_dev.bronze.returns
# MAGIC GROUP BY _source_file_name, source_region

# COMMAND ----------

# DBTITLE 1,Source region coverage across all tables
# MAGIC %sql
# MAGIC -- Check 9: Regional Coverage Validation
# MAGIC -- PURPOSE: Verify ALL regions (APAC, EMEA, LATAM, NAM) present in EVERY entity
# MAGIC -- USE: Ensures global data completeness - no regions were skipped during ingestion
# MAGIC -- EXPECTED: Each entity has data from all 4 regions
# MAGIC -- ACTION: Missing regions indicate pipeline configuration issues or source data gaps
# MAGIC     
# MAGIC SELECT 'customers'    AS entity, source_region, COUNT(*) AS rows FROM globalmart_dev.bronze.customers    GROUP BY source_region
# MAGIC UNION ALL
# MAGIC SELECT 'orders',       source_region, COUNT(*) FROM globalmart_dev.bronze.orders       GROUP BY source_region
# MAGIC UNION ALL
# MAGIC SELECT 'transactions', source_region, COUNT(*) FROM globalmart_dev.bronze.transactions GROUP BY source_region
# MAGIC UNION ALL
# MAGIC SELECT 'returns',      source_region, COUNT(*) FROM globalmart_dev.bronze.returns      GROUP BY source_region
# MAGIC UNION ALL
# MAGIC SELECT 'products',     source_region, COUNT(*) FROM globalmart_dev.bronze.products     GROUP BY source_region
# MAGIC UNION ALL
# MAGIC SELECT 'vendors',      source_region, COUNT(*) FROM globalmart_dev.bronze.vendors      GROUP BY source_region
# MAGIC ORDER BY entity, source_region

# COMMAND ----------

# DBTITLE 1,Audit columns populated on every record
# MAGIC %sql
# MAGIC -- Check 10: Audit Metadata Completeness
# MAGIC -- PURPOSE: Verify Auto Loader audit columns (_source_file_path, _source_file_name,
# MAGIC --          _source_modified_time, _ingested_at) are populated for ALL records
# MAGIC -- USE: Ensures data lineage, traceability, and troubleshooting capabilities
# MAGIC -- EXPECTED: total_rows equals count for each audit column (no NULLs)
# MAGIC -- ACTION: Missing audit data indicates Auto Loader configuration issues
# MAGIC     
# MAGIC SELECT
# MAGIC     'customers' AS entity,
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(_source_file_path)     AS has_file_path,
# MAGIC     COUNT(_source_file_name)     AS has_file_name,
# MAGIC     COUNT(_source_modified_time) AS has_modified_time,
# MAGIC     COUNT(_ingested_at)          AS has_ingested_at
# MAGIC FROM globalmart_dev.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT 'orders', COUNT(*), COUNT(_source_file_path), COUNT(_source_file_name), COUNT(_source_modified_time), COUNT(_ingested_at) FROM globalmart_dev.bronze.orders
# MAGIC UNION ALL
# MAGIC SELECT 'transactions', COUNT(*), COUNT(_source_file_path), COUNT(_source_file_name), COUNT(_source_modified_time), COUNT(_ingested_at) FROM globalmart_dev.bronze.transactions
# MAGIC UNION ALL
# MAGIC SELECT 'returns', COUNT(*), COUNT(_source_file_path), COUNT(_source_file_name), COUNT(_source_modified_time), COUNT(_ingested_at) FROM globalmart_dev.bronze.returns
# MAGIC UNION ALL
# MAGIC SELECT 'products', COUNT(*), COUNT(_source_file_path), COUNT(_source_file_name), COUNT(_source_modified_time), COUNT(_ingested_at) FROM globalmart_dev.bronze.products
# MAGIC UNION ALL
# MAGIC SELECT 'vendors', COUNT(*), COUNT(_source_file_path), COUNT(_source_file_name), COUNT(_source_modified_time), COUNT(_ingested_at) FROM globalmart_dev.bronze.vendors

# COMMAND ----------

# DBTITLE 1,Idempotency check
# MAGIC %sql
# MAGIC -- Check 11: Idempotency Validation
# MAGIC -- PURPOSE: Verify row counts remain stable across multiple pipeline runs
# MAGIC -- USE: Re-run this after triggering pipeline again - counts should NOT increase
# MAGIC -- EXPECTED: Same row counts as Check 1 (Auto Loader tracks processed files)
# MAGIC -- ACTION: If counts increase, investigate checkpoint issues or duplicate processing
# MAGIC -- NOTE: This is a re-run test - compare results with Check 1 baseline
# MAGIC     
# MAGIC SELECT 'customers'    AS entity, COUNT(*) AS rows FROM globalmart_dev.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT 'orders',       COUNT(*) FROM globalmart_dev.bronze.orders
# MAGIC UNION ALL
# MAGIC SELECT 'transactions', COUNT(*) FROM globalmart_dev.bronze.transactions
# MAGIC UNION ALL
# MAGIC SELECT 'returns',      COUNT(*) FROM globalmart_dev.bronze.returns
# MAGIC UNION ALL
# MAGIC SELECT 'products',     COUNT(*) FROM globalmart_dev.bronze.products
# MAGIC UNION ALL
# MAGIC SELECT 'vendors',      COUNT(*) FROM globalmart_dev.bronze.vendors