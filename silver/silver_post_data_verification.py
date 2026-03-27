# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Silver Data Verification Notebook
# MAGIC
# MAGIC ## Purpose
# MAGIC This notebook performs comprehensive data quality verification for the **globalmart_dev.silver** schema, including:
# MAGIC
# MAGIC - ✅ **Row Count Validation**: Compare silver tables vs quarantine tables
# MAGIC - ✅ **Data Quality Metrics**: Validate business rules and constraints
# MAGIC - ✅ **Quarantine Analysis**: Review rejected records and failure reasons
# MAGIC - ✅ **Referential Integrity**: Check for orphaned records across tables
# MAGIC - ✅ **Business Metrics**: Verify sales, refunds, and quantity summaries
# MAGIC - ✅ **Temporal Analysis**: Monitor data freshness and load patterns
# MAGIC - ✅ **Trend Visualizations**: Identify data quality issues over time
# MAGIC
# MAGIC ## Schema
# MAGIC **Catalog**: `globalmart_dev`  
# MAGIC **Schema**: `silver`
# MAGIC
# MAGIC ## Tables Verified
# MAGIC - `customers_silver` / `customers_quarantine`
# MAGIC - `orders_silver` / `orders_quarantine`
# MAGIC - `products_silver` / `products_quarantine`
# MAGIC - `returns_silver` / `returns_quarantine`
# MAGIC - `transactions_silver` / `transactions_quarantine`

# COMMAND ----------

# DBTITLE 1,Configuration
# Configuration
catalog = "globalmart_dev"
schema = "silver"

# Set default catalog and schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"✓ Using catalog: {catalog}")
print(f"✓ Using schema: {schema}")
print(f"\n📊 Verification started at: {spark.sql('SELECT current_timestamp()').collect()[0][0]}")

# COMMAND ----------

# DBTITLE 1,Row Count Comparison
# MAGIC %sql
# MAGIC -- Row Count Comparison: Silver vs Quarantine Tables
# MAGIC SELECT 
# MAGIC   'Customers' AS entity,
# MAGIC   (SELECT COUNT(*) FROM globalmart_dev.silver.customers_silver) AS silver_count,
# MAGIC   (SELECT COUNT(*) FROM globalmart_dev.silver.customers_quarantine) AS quarantine_count,
# MAGIC   ROUND((SELECT COUNT(*) FROM globalmart_dev.silver.customers_quarantine) * 100.0 / 
# MAGIC         NULLIF((SELECT COUNT(*) FROM globalmart_dev.silver.customers_silver) + 
# MAGIC                (SELECT COUNT(*) FROM globalmart_dev.silver.customers_quarantine), 0), 2) AS quarantine_rate_pct
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Orders' AS entity,
# MAGIC   (SELECT COUNT(*) FROM globalmart_dev.silver.orders_silver) AS silver_count,
# MAGIC   (SELECT COUNT(*) FROM globalmart_dev.silver.orders_quarantine) AS quarantine_count,
# MAGIC   ROUND((SELECT COUNT(*) FROM globalmart_dev.silver.orders_quarantine) * 100.0 / 
# MAGIC         NULLIF((SELECT COUNT(*) FROM globalmart_dev.silver.orders_silver) + 
# MAGIC                (SELECT COUNT(*) FROM globalmart_dev.silver.orders_quarantine), 0), 2) AS quarantine_rate_pct
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Products' AS entity,
# MAGIC   (SELECT COUNT(*) FROM globalmart_dev.silver.products_silver) AS silver_count,
# MAGIC   (SELECT COUNT(*) FROM globalmart_dev.silver.products_quarantine) AS quarantine_count,
# MAGIC   ROUND((SELECT COUNT(*) FROM globalmart_dev.silver.products_quarantine) * 100.0 / 
# MAGIC         NULLIF((SELECT COUNT(*) FROM globalmart_dev.silver.products_silver) + 
# MAGIC                (SELECT COUNT(*) FROM globalmart_dev.silver.products_quarantine), 0), 2) AS quarantine_rate_pct
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Returns' AS entity,
# MAGIC   (SELECT COUNT(*) FROM globalmart_dev.silver.returns_silver) AS silver_count,
# MAGIC   (SELECT COUNT(*) FROM globalmart_dev.silver.returns_quarantine) AS quarantine_count,
# MAGIC   ROUND((SELECT COUNT(*) FROM globalmart_dev.silver.returns_quarantine) * 100.0 / 
# MAGIC         NULLIF((SELECT COUNT(*) FROM globalmart_dev.silver.returns_silver) + 
# MAGIC                (SELECT COUNT(*) FROM globalmart_dev.silver.returns_quarantine), 0), 2) AS quarantine_rate_pct
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Transactions' AS entity,
# MAGIC   (SELECT COUNT(*) FROM globalmart_dev.silver.transactions_silver) AS silver_count,
# MAGIC   (SELECT COUNT(*) FROM globalmart_dev.silver.transactions_quarantine) AS quarantine_count,
# MAGIC   ROUND((SELECT COUNT(*) FROM globalmart_dev.silver.transactions_quarantine) * 100.0 / 
# MAGIC         NULLIF((SELECT COUNT(*) FROM globalmart_dev.silver.transactions_silver) + 
# MAGIC                (SELECT COUNT(*) FROM globalmart_dev.silver.transactions_quarantine), 0), 2) AS quarantine_rate_pct
# MAGIC
# MAGIC ORDER BY entity

# COMMAND ----------

# DBTITLE 1,Data Quality Summary
# MAGIC %sql
# MAGIC -- Overall Data Quality Score Summary
# MAGIC WITH quality_metrics AS (
# MAGIC   SELECT 
# MAGIC     'Customers' AS entity,
# MAGIC     COUNT(*) AS total_records,
# MAGIC     SUM(CASE WHEN customer_id IS NOT NULL THEN 1 ELSE 0 END) AS valid_ids,
# MAGIC     SUM(CASE WHEN email LIKE '%@%' THEN 1 ELSE 0 END) AS valid_emails,
# MAGIC     SUM(CASE WHEN phone_number IS NOT NULL THEN 1 ELSE 0 END) AS has_phone
# MAGIC   FROM globalmart_dev.silver.customers_silver
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   SELECT 
# MAGIC     'Orders' AS entity,
# MAGIC     COUNT(*) AS total_records,
# MAGIC     SUM(CASE WHEN order_id IS NOT NULL THEN 1 ELSE 0 END) AS valid_ids,
# MAGIC     SUM(CASE WHEN order_value > 0 THEN 1 ELSE 0 END) AS positive_values,
# MAGIC     SUM(CASE WHEN customer_id IS NOT NULL THEN 1 ELSE 0 END) AS has_customer
# MAGIC   FROM globalmart_dev.silver.orders_silver
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   SELECT 
# MAGIC     'Products' AS entity,
# MAGIC     COUNT(*) AS total_records,
# MAGIC     SUM(CASE WHEN product_id IS NOT NULL THEN 1 ELSE 0 END) AS valid_ids,
# MAGIC     SUM(CASE WHEN price > 0 THEN 1 ELSE 0 END) AS positive_prices,
# MAGIC     SUM(CASE WHEN stock_quantity >= 0 THEN 1 ELSE 0 END) AS valid_stock
# MAGIC   FROM globalmart_dev.silver.products_silver
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   SELECT 
# MAGIC     'Returns' AS entity,
# MAGIC     COUNT(*) AS total_records,
# MAGIC     SUM(CASE WHEN return_id IS NOT NULL THEN 1 ELSE 0 END) AS valid_ids,
# MAGIC     SUM(CASE WHEN refund_amount >= 0 THEN 1 ELSE 0 END) AS valid_refunds,
# MAGIC     SUM(CASE WHEN order_id IS NOT NULL THEN 1 ELSE 0 END) AS has_order
# MAGIC   FROM globalmart_dev.silver.returns_silver
# MAGIC   
# MAGIC   UNION ALL
# MAGIC   
# MAGIC   SELECT 
# MAGIC     'Transactions' AS entity,
# MAGIC     COUNT(*) AS total_records,
# MAGIC     SUM(CASE WHEN transaction_id IS NOT NULL THEN 1 ELSE 0 END) AS valid_ids,
# MAGIC     SUM(CASE WHEN transaction_amount > 0 THEN 1 ELSE 0 END) AS positive_amounts,
# MAGIC     SUM(CASE WHEN order_id IS NOT NULL THEN 1 ELSE 0 END) AS has_order
# MAGIC   FROM globalmart_dev.silver.transactions_silver
# MAGIC )
# MAGIC SELECT 
# MAGIC   entity,
# MAGIC   total_records,
# MAGIC   valid_ids,
# MAGIC   ROUND(valid_ids * 100.0 / NULLIF(total_records, 0), 2) AS id_completeness_pct,
# MAGIC   ROUND((valid_emails + positive_values + positive_prices + valid_refunds + positive_amounts) * 100.0 / NULLIF(total_records, 0), 2) AS quality_metric_2_pct,
# MAGIC   ROUND((has_phone + has_customer + valid_stock + has_order + has_order) * 100.0 / NULLIF(total_records, 0), 2) AS quality_metric_3_pct
# MAGIC FROM quality_metrics
# MAGIC ORDER BY entity

# COMMAND ----------

# DBTITLE 1,Customers Quality Check
# MAGIC %sql
# MAGIC -- Customers: Quality Metrics & Quarantine Samples
# MAGIC SELECT '=== CUSTOMERS SILVER: Quality Metrics ===' AS section
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Total Records: ', CAST(COUNT(*) AS STRING)) FROM globalmart_dev.silver.customers_silver
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Valid Emails: ', CAST(SUM(CASE WHEN email LIKE '%@%' THEN 1 ELSE 0 END) AS STRING)) FROM globalmart_dev.silver.customers_silver
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Missing Phone: ', CAST(SUM(CASE WHEN phone_number IS NULL THEN 1 ELSE 0 END) AS STRING)) FROM globalmart_dev.silver.customers_silver
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Duplicate Customer IDs: ', CAST(COUNT(*) - COUNT(DISTINCT customer_id) AS STRING)) FROM globalmart_dev.silver.customers_silver;
# MAGIC
# MAGIC -- Sample quarantine records
# MAGIC SELECT 
# MAGIC   '=== CUSTOMERS QUARANTINE: Sample Issues ===' AS section,
# MAGIC   NULL AS customer_id, 
# MAGIC   NULL AS customer_name,
# MAGIC   NULL AS email,
# MAGIC   NULL AS issue_type
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   '' AS section,
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   email,
# MAGIC   CASE 
# MAGIC     WHEN email NOT LIKE '%@%' THEN 'Invalid Email'
# MAGIC     WHEN customer_id IS NULL THEN 'Missing Customer ID'
# MAGIC     ELSE 'Other Validation Failure'
# MAGIC   END AS issue_type
# MAGIC FROM globalmart_dev.silver.customers_quarantine
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Orders Quality Check
# MAGIC %sql
# MAGIC -- Orders: Quality Metrics & Quarantine Samples
# MAGIC SELECT '=== ORDERS SILVER: Quality Metrics ===' AS section
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Total Orders: ', CAST(COUNT(*) AS STRING)) FROM globalmart_dev.silver.orders_silver
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Total Order Value: $', CAST(ROUND(SUM(order_value), 2) AS STRING)) FROM globalmart_dev.silver.orders_silver
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Avg Order Value: $', CAST(ROUND(AVG(order_value), 2) AS STRING)) FROM globalmart_dev.silver.orders_silver
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Orders with Zero/Negative Value: ', CAST(SUM(CASE WHEN order_value <= 0 THEN 1 ELSE 0 END) AS STRING)) FROM globalmart_dev.silver.orders_silver
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Orders Missing Customer: ', CAST(SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS STRING)) FROM globalmart_dev.silver.orders_silver;
# MAGIC
# MAGIC -- Sample quarantine records
# MAGIC SELECT 
# MAGIC   '=== ORDERS QUARANTINE: Sample Issues ===' AS section,
# MAGIC   NULL AS order_id, 
# MAGIC   NULL AS customer_id,
# MAGIC   NULL AS order_value,
# MAGIC   NULL AS issue_type
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   '' AS section,
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_value,
# MAGIC   CASE 
# MAGIC     WHEN order_value <= 0 THEN 'Non-Positive Order Value'
# MAGIC     WHEN order_id IS NULL THEN 'Missing Order ID'
# MAGIC     WHEN customer_id IS NULL THEN 'Missing Customer ID'
# MAGIC     ELSE 'Other Validation Failure'
# MAGIC   END AS issue_type
# MAGIC FROM globalmart_dev.silver.orders_quarantine
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Products Quality Check
# MAGIC %sql
# MAGIC -- Products: Quality Metrics & Quarantine Samples
# MAGIC SELECT '=== PRODUCTS SILVER: Quality Metrics ===' AS section
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Total Products: ', CAST(COUNT(*) AS STRING)) FROM globalmart_dev.silver.products_silver
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Avg Product Price: $', CAST(ROUND(AVG(price), 2) AS STRING)) FROM globalmart_dev.silver.products_silver
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Products with Zero/Negative Price: ', CAST(SUM(CASE WHEN price <= 0 THEN 1 ELSE 0 END) AS STRING)) FROM globalmart_dev.silver.products_silver
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Out of Stock Products: ', CAST(SUM(CASE WHEN stock_quantity = 0 THEN 1 ELSE 0 END) AS STRING)) FROM globalmart_dev.silver.products_silver
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Products with Negative Stock: ', CAST(SUM(CASE WHEN stock_quantity < 0 THEN 1 ELSE 0 END) AS STRING)) FROM globalmart_dev.silver.products_silver;
# MAGIC
# MAGIC -- Sample quarantine records
# MAGIC SELECT 
# MAGIC   '=== PRODUCTS QUARANTINE: Sample Issues ===' AS section,
# MAGIC   NULL AS product_id, 
# MAGIC   NULL AS product_name,
# MAGIC   NULL AS price,
# MAGIC   NULL AS stock_quantity,
# MAGIC   NULL AS issue_type
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   '' AS section,
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   price,
# MAGIC   stock_quantity,
# MAGIC   CASE 
# MAGIC     WHEN price <= 0 THEN 'Non-Positive Price'
# MAGIC     WHEN stock_quantity < 0 THEN 'Negative Stock'
# MAGIC     WHEN product_id IS NULL THEN 'Missing Product ID'
# MAGIC     ELSE 'Other Validation Failure'
# MAGIC   END AS issue_type
# MAGIC FROM globalmart_dev.silver.products_quarantine
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Returns Quality Check
# MAGIC %sql
# MAGIC -- Returns: Quality Metrics & Quarantine Samples
# MAGIC SELECT '=== RETURNS SILVER: Quality Metrics ===' AS section
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Total Returns: ', CAST(COUNT(*) AS STRING)) FROM globalmart_dev.silver.returns_silver
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Total Refund Amount: $', CAST(ROUND(SUM(refund_amount), 2) AS STRING)) FROM globalmart_dev.silver.returns_silver
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Avg Refund Amount: $', CAST(ROUND(AVG(refund_amount), 2) AS STRING)) FROM globalmart_dev.silver.returns_silver
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Returns with Negative Refund: ', CAST(SUM(CASE WHEN refund_amount < 0 THEN 1 ELSE 0 END) AS STRING)) FROM globalmart_dev.silver.returns_silver
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Returns Missing Order: ', CAST(SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS STRING)) FROM globalmart_dev.silver.returns_silver;
# MAGIC
# MAGIC -- Sample quarantine records
# MAGIC SELECT 
# MAGIC   '=== RETURNS QUARANTINE: Sample Issues ===' AS section,
# MAGIC   NULL AS return_id, 
# MAGIC   NULL AS order_id,
# MAGIC   NULL AS refund_amount,
# MAGIC   NULL AS issue_type
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   '' AS section,
# MAGIC   return_id,
# MAGIC   order_id,
# MAGIC   refund_amount,
# MAGIC   CASE 
# MAGIC     WHEN refund_amount < 0 THEN 'Negative Refund Amount'
# MAGIC     WHEN return_id IS NULL THEN 'Missing Return ID'
# MAGIC     WHEN order_id IS NULL THEN 'Missing Order ID'
# MAGIC     ELSE 'Other Validation Failure'
# MAGIC   END AS issue_type
# MAGIC FROM globalmart_dev.silver.returns_quarantine
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Transactions Quality Check
# MAGIC %sql
# MAGIC -- Transactions: Quality Metrics & Quarantine Samples
# MAGIC SELECT '=== TRANSACTIONS SILVER: Quality Metrics ===' AS section
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Total Transactions: ', CAST(COUNT(*) AS STRING)) FROM globalmart_dev.silver.transactions_silver
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Total Transaction Amount: $', CAST(ROUND(SUM(transaction_amount), 2) AS STRING)) FROM globalmart_dev.silver.transactions_silver
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Avg Transaction Amount: $', CAST(ROUND(AVG(transaction_amount), 2) AS STRING)) FROM globalmart_dev.silver.transactions_silver
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Transactions with Zero/Negative Amount: ', CAST(SUM(CASE WHEN transaction_amount <= 0 THEN 1 ELSE 0 END) AS STRING)) FROM globalmart_dev.silver.transactions_silver
# MAGIC UNION ALL
# MAGIC SELECT CONCAT('Transactions Missing Order: ', CAST(SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS STRING)) FROM globalmart_dev.silver.transactions_silver;
# MAGIC
# MAGIC -- Sample quarantine records
# MAGIC SELECT 
# MAGIC   '=== TRANSACTIONS QUARANTINE: Sample Issues ===' AS section,
# MAGIC   NULL AS transaction_id, 
# MAGIC   NULL AS order_id,
# MAGIC   NULL AS transaction_amount,
# MAGIC   NULL AS payment_method,
# MAGIC   NULL AS issue_type
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   '' AS section,
# MAGIC   transaction_id,
# MAGIC   order_id,
# MAGIC   transaction_amount,
# MAGIC   payment_method,
# MAGIC   CASE 
# MAGIC     WHEN transaction_amount <= 0 THEN 'Non-Positive Transaction Amount'
# MAGIC     WHEN transaction_id IS NULL THEN 'Missing Transaction ID'
# MAGIC     WHEN order_id IS NULL THEN 'Missing Order ID'
# MAGIC     ELSE 'Other Validation Failure'
# MAGIC   END AS issue_type
# MAGIC FROM globalmart_dev.silver.transactions_quarantine
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Referential Integrity
# MAGIC %sql
# MAGIC -- Referential Integrity: Orphaned Records Check
# MAGIC SELECT 
# MAGIC   'Orphaned Orders (Customer Not Found)' AS integrity_check,
# MAGIC   COUNT(*) AS orphaned_count
# MAGIC FROM globalmart_dev.silver.orders_silver o
# MAGIC LEFT JOIN globalmart_dev.silver.customers_silver c ON o.customer_id = c.customer_id
# MAGIC WHERE c.customer_id IS NULL AND o.customer_id IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Orphaned Returns (Order Not Found)' AS integrity_check,
# MAGIC   COUNT(*) AS orphaned_count
# MAGIC FROM globalmart_dev.silver.returns_silver r
# MAGIC LEFT JOIN globalmart_dev.silver.orders_silver o ON r.order_id = o.order_id
# MAGIC WHERE o.order_id IS NULL AND r.order_id IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Orphaned Transactions (Order Not Found)' AS integrity_check,
# MAGIC   COUNT(*) AS orphaned_count
# MAGIC FROM globalmart_dev.silver.transactions_silver t
# MAGIC LEFT JOIN globalmart_dev.silver.orders_silver o ON t.order_id = o.order_id
# MAGIC WHERE o.order_id IS NULL AND t.order_id IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Orders with Multiple Transactions' AS integrity_check,
# MAGIC   COUNT(DISTINCT order_id) AS orphaned_count
# MAGIC FROM (
# MAGIC   SELECT order_id, COUNT(*) AS txn_count
# MAGIC   FROM globalmart_dev.silver.transactions_silver
# MAGIC   GROUP BY order_id
# MAGIC   HAVING COUNT(*) > 1
# MAGIC )
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Returns Exceeding Order Value' AS integrity_check,
# MAGIC   COUNT(*) AS orphaned_count
# MAGIC FROM globalmart_dev.silver.returns_silver r
# MAGIC INNER JOIN globalmart_dev.silver.orders_silver o ON r.order_id = o.order_id
# MAGIC WHERE r.refund_amount > o.order_value

# COMMAND ----------

# DBTITLE 1,Business Metrics
# MAGIC %sql
# MAGIC -- Business Metrics: Sales, Refunds, Quantities
# MAGIC WITH metrics AS (
# MAGIC   SELECT 
# MAGIC     COUNT(DISTINCT c.customer_id) AS total_customers,
# MAGIC     COUNT(DISTINCT o.order_id) AS total_orders,
# MAGIC     COALESCE(SUM(o.order_value), 0) AS total_sales,
# MAGIC     COALESCE(SUM(t.transaction_amount), 0) AS total_transactions,
# MAGIC     COALESCE(SUM(r.refund_amount), 0) AS total_refunds,
# MAGIC     COUNT(DISTINCT r.return_id) AS total_returns,
# MAGIC     COUNT(DISTINCT p.product_id) AS total_products
# MAGIC   FROM globalmart_dev.silver.customers_silver c
# MAGIC   LEFT JOIN globalmart_dev.silver.orders_silver o ON c.customer_id = o.customer_id
# MAGIC   LEFT JOIN globalmart_dev.silver.transactions_silver t ON o.order_id = t.order_id
# MAGIC   LEFT JOIN globalmart_dev.silver.returns_silver r ON o.order_id = r.order_id
# MAGIC   CROSS JOIN (SELECT COUNT(DISTINCT product_id) AS product_id FROM globalmart_dev.silver.products_silver) p
# MAGIC )
# MAGIC SELECT 
# MAGIC   'Total Customers' AS metric, CAST(total_customers AS STRING) AS value FROM metrics
# MAGIC UNION ALL
# MAGIC SELECT 'Total Orders' AS metric, CAST(total_orders AS STRING) AS value FROM metrics
# MAGIC UNION ALL
# MAGIC SELECT 'Total Products' AS metric, CAST(total_products AS STRING) AS value FROM metrics
# MAGIC UNION ALL
# MAGIC SELECT 'Total Sales' AS metric, CONCAT('$', CAST(ROUND(total_sales, 2) AS STRING)) AS value FROM metrics
# MAGIC UNION ALL
# MAGIC SELECT 'Total Transactions Processed' AS metric, CONCAT('$', CAST(ROUND(total_transactions, 2) AS STRING)) AS value FROM metrics
# MAGIC UNION ALL
# MAGIC SELECT 'Total Refunds' AS metric, CONCAT('$', CAST(ROUND(total_refunds, 2) AS STRING)) AS value FROM metrics
# MAGIC UNION ALL
# MAGIC SELECT 'Total Returns' AS metric, CAST(total_returns AS STRING) AS value FROM metrics
# MAGIC UNION ALL
# MAGIC SELECT 'Net Revenue (Sales - Refunds)' AS metric, CONCAT('$', CAST(ROUND(total_sales - total_refunds, 2) AS STRING)) AS value FROM metrics
# MAGIC UNION ALL
# MAGIC SELECT 'Return Rate' AS metric, CONCAT(CAST(ROUND(total_returns * 100.0 / NULLIF(total_orders, 0), 2) AS STRING), '%') AS value FROM metrics
# MAGIC UNION ALL
# MAGIC SELECT 'Avg Order Value' AS metric, CONCAT('$', CAST(ROUND(total_sales / NULLIF(total_orders, 0), 2) AS STRING)) AS value FROM metrics

# COMMAND ----------

# DBTITLE 1,Temporal Analysis
# MAGIC %sql
# MAGIC -- Temporal Analysis: Data Freshness & Load Patterns
# MAGIC SELECT 
# MAGIC   'customers_silver' AS table_name,
# MAGIC   MAX(updated_at) AS latest_record_timestamp,
# MAGIC   MIN(updated_at) AS earliest_record_timestamp,
# MAGIC   DATEDIFF(DAY, MIN(updated_at), MAX(updated_at)) AS date_range_days
# MAGIC FROM globalmart_dev.silver.customers_silver
# MAGIC WHERE updated_at IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'orders_silver' AS table_name,
# MAGIC   MAX(order_date) AS latest_record_timestamp,
# MAGIC   MIN(order_date) AS earliest_record_timestamp,
# MAGIC   DATEDIFF(DAY, MIN(order_date), MAX(order_date)) AS date_range_days
# MAGIC FROM globalmart_dev.silver.orders_silver
# MAGIC WHERE order_date IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'products_silver' AS table_name,
# MAGIC   MAX(updated_at) AS latest_record_timestamp,
# MAGIC   MIN(updated_at) AS earliest_record_timestamp,
# MAGIC   DATEDIFF(DAY, MIN(updated_at), MAX(updated_at)) AS date_range_days
# MAGIC FROM globalmart_dev.silver.products_silver
# MAGIC WHERE updated_at IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'returns_silver' AS table_name,
# MAGIC   MAX(return_date) AS latest_record_timestamp,
# MAGIC   MIN(return_date) AS earliest_record_timestamp,
# MAGIC   DATEDIFF(DAY, MIN(return_date), MAX(return_date)) AS date_range_days
# MAGIC FROM globalmart_dev.silver.returns_silver
# MAGIC WHERE return_date IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'transactions_silver' AS table_name,
# MAGIC   MAX(transaction_date) AS latest_record_timestamp,
# MAGIC   MIN(transaction_date) AS earliest_record_timestamp,
# MAGIC   DATEDIFF(DAY, MIN(transaction_date), MAX(transaction_date)) AS date_range_days
# MAGIC FROM globalmart_dev.silver.transactions_silver
# MAGIC WHERE transaction_date IS NOT NULL
# MAGIC
# MAGIC ORDER BY table_name

# COMMAND ----------

# DBTITLE 1,Quality Trends Visualization
# Data Quality Trends Visualization
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Fetch quarantine rates
query = """
SELECT 
  'Customers' AS entity,
  (SELECT COUNT(*) FROM globalmart_dev.silver.customers_silver) AS silver_count,
  (SELECT COUNT(*) FROM globalmart_dev.silver.customers_quarantine) AS quarantine_count
UNION ALL
SELECT 'Orders', 
  (SELECT COUNT(*) FROM globalmart_dev.silver.orders_silver),
  (SELECT COUNT(*) FROM globalmart_dev.silver.orders_quarantine)
UNION ALL
SELECT 'Products',
  (SELECT COUNT(*) FROM globalmart_dev.silver.products_silver),
  (SELECT COUNT(*) FROM globalmart_dev.silver.products_quarantine)
UNION ALL
SELECT 'Returns',
  (SELECT COUNT(*) FROM globalmart_dev.silver.returns_silver),
  (SELECT COUNT(*) FROM globalmart_dev.silver.returns_quarantine)
UNION ALL
SELECT 'Transactions',
  (SELECT COUNT(*) FROM globalmart_dev.silver.transactions_silver),
  (SELECT COUNT(*) FROM globalmart_dev.silver.transactions_quarantine)
"""

df = spark.sql(query).toPandas()
df['quarantine_rate'] = (df['quarantine_count'] / (df['silver_count'] + df['quarantine_count'])) * 100

# Create visualization
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

# Bar chart: Quarantine rates
ax1.bar(df['entity'], df['quarantine_rate'], color='coral')
ax1.set_title('Quarantine Rate by Entity', fontsize=14, fontweight='bold')
ax1.set_xlabel('Entity')
ax1.set_ylabel('Quarantine Rate (%)')
ax1.axhline(y=5, color='red', linestyle='--', label='5% Threshold')
ax1.legend()
ax1.grid(axis='y', alpha=0.3)

# Stacked bar: Silver vs Quarantine counts
ax2.bar(df['entity'], df['silver_count'], label='Silver (Valid)', color='lightgreen')
ax2.bar(df['entity'], df['quarantine_count'], bottom=df['silver_count'], label='Quarantine (Invalid)', color='salmon')
ax2.set_title('Record Distribution: Silver vs Quarantine', fontsize=14, fontweight='bold')
ax2.set_xlabel('Entity')
ax2.set_ylabel('Record Count')
ax2.legend()
ax2.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.show()

print("\n📊 Data Quality Summary:")
print(df[['entity', 'silver_count', 'quarantine_count', 'quarantine_rate']].to_string(index=False))

# COMMAND ----------

# DBTITLE 1,Quarantine Analysis
# Quarantine Rate Analysis & Alerts
import pandas as pd

# Define quarantine rate threshold
ALERT_THRESHOLD = 5.0  # Alert if quarantine rate exceeds 5%

# Fetch data
query = """
SELECT 
  'Customers' AS entity,
  (SELECT COUNT(*) FROM globalmart_dev.silver.customers_quarantine) AS quarantine_count,
  (SELECT COUNT(*) FROM globalmart_dev.silver.customers_silver) + 
  (SELECT COUNT(*) FROM globalmart_dev.silver.customers_quarantine) AS total_count
UNION ALL
SELECT 'Orders',
  (SELECT COUNT(*) FROM globalmart_dev.silver.orders_quarantine),
  (SELECT COUNT(*) FROM globalmart_dev.silver.orders_silver) +
  (SELECT COUNT(*) FROM globalmart_dev.silver.orders_quarantine)
UNION ALL
SELECT 'Products',
  (SELECT COUNT(*) FROM globalmart_dev.silver.products_quarantine),
  (SELECT COUNT(*) FROM globalmart_dev.silver.products_silver) +
  (SELECT COUNT(*) FROM globalmart_dev.silver.products_quarantine)
UNION ALL
SELECT 'Returns',
  (SELECT COUNT(*) FROM globalmart_dev.silver.returns_quarantine),
  (SELECT COUNT(*) FROM globalmart_dev.silver.returns_silver) +
  (SELECT COUNT(*) FROM globalmart_dev.silver.returns_quarantine)
UNION ALL
SELECT 'Transactions',
  (SELECT COUNT(*) FROM globalmart_dev.silver.transactions_quarantine),
  (SELECT COUNT(*) FROM globalmart_dev.silver.transactions_silver) +
  (SELECT COUNT(*) FROM globalmart_dev.silver.transactions_quarantine)
"""

df = spark.sql(query).toPandas()
df['quarantine_rate'] = (df['quarantine_count'] / df['total_count']) * 100
df['status'] = df['quarantine_rate'].apply(lambda x: '🚨 ALERT' if x > ALERT_THRESHOLD else '✅ OK')

print("\n" + "="*70)
print("🔍 QUARANTINE RATE ANALYSIS")
print("="*70)
print(f"\nAlert Threshold: {ALERT_THRESHOLD}%\n")
print(df[['entity', 'quarantine_count', 'total_count', 'quarantine_rate', 'status']].to_string(index=False))

# Generate alerts
alerts = df[df['quarantine_rate'] > ALERT_THRESHOLD]
if not alerts.empty:
    print("\n" + "="*70)
    print("🚨 ALERTS DETECTED")
    print("="*70)
    for _, row in alerts.iterrows():
        print(f"\n⚠️  {row['entity']}: {row['quarantine_rate']:.2f}% quarantine rate")
        print(f"   → {row['quarantine_count']} records quarantined out of {row['total_count']} total")
        print(f"   → Action: Review {row['entity'].lower()}_quarantine table for root causes")
else:
    print("\n✅ No alerts. All quarantine rates are within acceptable limits.")

print("\n" + "="*70)

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC # Verification Summary
# MAGIC
# MAGIC ## ✅ Checks Completed
# MAGIC
# MAGIC 1. **Row Count Validation** - Compared silver tables vs quarantine tables
# MAGIC 2. **Data Quality Metrics** - Validated completeness and business rules
# MAGIC 3. **Quarantine Analysis** - Reviewed rejected records and failure patterns
# MAGIC 4. **Referential Integrity** - Checked for orphaned records
# MAGIC 5. **Business Metrics** - Verified sales, refunds, and revenue calculations
# MAGIC 6. **Temporal Analysis** - Monitored data freshness and coverage
# MAGIC 7. **Trend Visualization** - Identified quality patterns
# MAGIC 8. **Alert Monitoring** - Flagged entities exceeding quarantine thresholds
# MAGIC
# MAGIC ## 📋 Next Steps
# MAGIC
# MAGIC - **Review Quarantine Tables**: Investigate root causes for rejected records
# MAGIC - **Fix Data Quality Issues**: Address validation failures at the source
# MAGIC - **Monitor Trends**: Run this notebook regularly to track quality over time
# MAGIC - **Update Expectations**: Adjust pipeline constraints based on findings
# MAGIC - **Document Patterns**: Record common issues and resolutions
# MAGIC
# MAGIC ## 🔗 Related Resources
# MAGIC
# MAGIC - **Silver Pipeline**: `/Workspace/Users/ayushkumar.singh@simbustech.com/GlobalMart_DataIntelligence/transformations/silver/`
# MAGIC - **Schema**: `globalmart_dev.silver`
# MAGIC - **Quarantine Tables**: All tables ending with `_quarantine`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC *Last executed: Check cell execution timestamps above*