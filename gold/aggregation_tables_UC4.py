# Databricks notebook source
# DBTITLE 1,Revenue by region
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW globalmart_dev.gold.mv_revenue_by_region AS
# MAGIC SELECT
# MAGIC     r.source_region,
# MAGIC     SUM(f.sales_amount) AS total_revenue,
# MAGIC     COUNT(DISTINCT f.order_id) AS total_orders,
# MAGIC     AVG(f.sales_amount) AS avg_order_value
# MAGIC FROM globalmart_dev.bronze.gold_fact_transactions_mv f
# MAGIC LEFT JOIN globalmart_dev.bronze.gold_dim_region r
# MAGIC     ON f.region_key = r.region_key
# MAGIC GROUP BY r.source_region;

# COMMAND ----------

# DBTITLE 1,Vendor Returns
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW globalmart_dev.gold.mv_vendor_returns AS
# MAGIC SELECT
# MAGIC     v.vendor_name,
# MAGIC     COUNT(*) AS total_returns,
# MAGIC     SUM(f.return_amount) AS total_return_value,
# MAGIC     AVG(f.return_to_purchase_ratio) AS avg_return_ratio
# MAGIC FROM globalmart_dev.bronze.gold_fact_returns f
# MAGIC LEFT JOIN globalmart_dev.bronze.gold_dim_vendor v
# MAGIC     ON f.vendor_key = v.vendor_key
# MAGIC GROUP BY v.vendor_name;

# COMMAND ----------

# DBTITLE 1,Slow moving products
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW globalmart_dev.gold.mv_inventory_velocity AS
# MAGIC SELECT
# MAGIC     p.product_name,
# MAGIC     SUM(f.sales_amount) AS total_sales,
# MAGIC     COUNT(f.order_id) AS order_count
# MAGIC FROM globalmart_dev.bronze.gold_fact_transactions_mv f
# MAGIC LEFT JOIN globalmart_dev.bronze.gold_dim_product p
# MAGIC     ON f.product_key = p.product_key
# MAGIC GROUP BY p.product_name;