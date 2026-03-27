# Databricks notebook source
# DBTITLE 1,Import Spark SQL Functions and Data Types for Pipeline
from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, when, expr, trim, initcap, lower, upper,row_number,
    regexp_extract, regexp_replace, split,
    to_date, lit, coalesce, nanvl, concat_ws, array_remove, array
)
from pyspark.sql.types import DoubleType

# COMMAND ----------

# DBTITLE 1,Generate Issue Type Labels from Condition Checks
def build_issue_type(checks: dict):
    """
    Build a comma-separated string listing all validation failures for a row.
    
    This helper is used in all quarantine tables to produce human-readable
    issue diagnostics. Each key in the checks dict is a label (e.g.
    "missing_order_id"), and each value is a Column expression that evaluates
    to TRUE when that specific issue is present in a row.
    
    Args:
        checks: dict of {label: Column expression that is TRUE when there is a problem}
    
    Returns:
        Column containing a comma-separated string of all firing labels.
        Returns empty string if no issues are detected.
    
    Example:
        For a row with null order_id and negative sales, returns:
        "missing_order_id, invalid_sales"
    """
    flags = [when(condition, lit(label)).otherwise(lit(None)) for label, condition in checks.items()]
    return concat_ws(", ", *[f for f in flags])


# COMMAND ----------

# DBTITLE 1,Define customer quarantine and silver tables with validation
# ── Quarantine rules ──────────────────────────────────────────────────────────
# Each rule defines a SQL condition that a *valid* customer row must satisfy.
# Any row failing at least one of these will be routed to the quarantine table.
rules_customers = {
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_customer_email": "customer_email IS NOT NULL",
    "valid_customer_name": "customer_name IS NOT NULL AND TRIM(customer_name) != ''",
    "valid_segment": "segment IN ('Home Office','Corporate','Consumer')",
    "valid_region":  "region IN ('North','South','East','West','Central')",
}

# Build a single SQL expression that is TRUE when a row violates *any* rule.
quarantine_rules_customers = "NOT ({})".format(" AND ".join(rules_customers.values()))


# ── Quarantine table ──────────────────────────────────────────────────────────
@dp.table(name="globalmart_dev.silver.customers_quarantine")
def customers_quarantine():
    """
    Captures every customer row that fails one or more validation rules.
    An `issue_type` column is added to record *which* rules were violated,
    making it easier to diagnose and reprocess bad data downstream.
    
    Note: customers_bronze_view already handles city/state swap fix for R4
    and segment/region normalization, so this quarantine runs on pre-cleaned data.
    """
    return (
        spark.readStream.table("customers_bronze_view")
        # Tag each row with a boolean indicating whether it is quarantined.
        .withColumn("is_quarantined", expr(quarantine_rules_customers))
        # Keep only the rows that failed validation.
        .filter("is_quarantined = true")
        # Derive a human-readable list of the specific issues for each bad row.
        .withColumn("issue_type", build_issue_type({
            "missing_customer_id":   col("customer_id").isNull(),
            "missing_customer_email": col("customer_email").isNull(),
            "missing_customer_name": col("customer_name").isNull() | (trim(col("customer_name")) == ""),
            "invalid_segment":       ~col("segment").isin("Consumer", "Corporate", "Home Office"),
            "invalid_region":        ~col("region").isin("North", "South", "East", "West", "Central"),
        }))
        # The helper column is no longer needed once filtering is done.
        .drop("is_quarantined")
    )


# ── Silver (clean) customers table ────────────────────────────────────────────
@dp.table(
    name="globalmart_dev.silver.customers_silver",
    comment="Cleaned and deduplicated customers. City/state swap fixed for R4. Segment and region normalised. Most complete copy kept per customer_id. All transformations handled in customers_bronze_view."
)
# Assert all rules pass for rows that reach the silver table.
# Delta Live Tables will surface any violations as data quality metrics.
@dp.expect_all_or_drop(rules_customers)
def customers_silver():
    """
    Produces a clean, deduplicated stream of customers by leveraging the
    bronze view's preprocessing:
      1. Column unification across regional datasets.
      2. R4 city/state swap fix.
      3. Segment and region normalization.
      4. Completeness score calculation.
      5. Deduplication (max_by on _completeness_score retains best record).
      6. Automatic filtering of quarantined rows.
    
    This silver table simply reads the final output from customers_bronze_view.
    """
    return spark.readStream.table("customers_bronze_view")

# COMMAND ----------

# DBTITLE 1,Define order quarantine and silver tables with validati ...
# ── Quarantine rules ──────────────────────────────────────────────────────────
# Each rule defines a SQL condition that a *valid* order row must satisfy.
# Any row failing at least one of these will be routed to the quarantine table.
rules_orders = {
    "valid_order_id":               "order_id IS NOT NULL",
    "valid_customer_id":            "customer_id IS NOT NULL",
    "valid_purchase_date":          "order_purchase_date IS NOT NULL",
    "valid_approved_date":          "order_approved_at IS NOT NULL",
    "valid_carrier_date":           "order_delivered_carrier_date IS NOT NULL",
    "valid_customer_delivery_date": "order_delivered_customer_date IS NOT NULL",
    "valid_order_status":           "order_status IN ('Processing','Shipped','Cancelled','Delivered','Created','Invoiced','Unavailable')",
    "valid_ship_mode":              "ship_mode IN ('Standard Class','Second Class','First Class','Same Day')",
}

# NOTE: order_estimated_delivery_date is intentionally excluded from quarantine.
# Region 5 (R5) is missing it for all rows, so failing this check would
# quarantine an entire legitimate dataset. It is warned on separately rather
# than used as a hard drop condition.

# Build a single SQL expression that is TRUE when a row violates *any* rule,
# i.e. the logical negation of all rules AND-ed together.
quarantine_rules_orders = "NOT ({})".format(" AND ".join(rules_orders.values()))


# ── Quarantine table ──────────────────────────────────────────────────────────
@dp.table(name="globalmart_dev.silver.orders_quarantine")
def orders_quarantine():
    """
    Captures every order row that fails one or more validation rules.
    An `issue_type` column is added to record *which* rules were violated,
    making it easier to diagnose and reprocess bad data downstream.
    """
    return (
        spark.readStream.table("orders_bronze_view")
        # Tag each row with a boolean indicating whether it is quarantined.
        .withColumn("is_quarantined", expr(quarantine_rules_orders))
        # Keep only the rows that failed validation.
        .filter("is_quarantined = true")
        # Derive a human-readable list of the specific issues for each bad row.
        .withColumn("issue_type", build_issue_type({
            "missing_order_id":      col("order_id").isNull(),
            "missing_customer_id":   col("customer_id").isNull(),
            "missing_purchase_date": col("order_purchase_date").isNull(),
            "missing_approved_date": col("order_approved_at").isNull(),
            "missing_carrier_date":  col("order_delivered_carrier_date").isNull(),
            "missing_delivery_date": col("order_delivered_customer_date").isNull(),
            "invalid_order_status":  ~col("order_status").isin(
                "Processing", "Shipped", "Cancelled", "Delivered",
                "Created", "Invoiced", "Unavailable"
            ),
            "invalid_ship_mode":     ~col("ship_mode").isin(
                "Standard Class", "Second Class", "First Class", "Same Day"
            ),
        }))
        # The helper column is no longer needed once filtering is done.
        .drop("is_quarantined")
    )


# ── Silver (clean) orders table ───────────────────────────────────────────────
@dp.table(
    name="globalmart_dev.silver.orders_silver",
    comment="Cleaned orders. Both date formats parsed. R5 estimated_delivery_date null is accepted."
)
# Assert all rules pass for rows that reach the silver table.
# Delta Live Tables will surface any violations as data quality metrics.
@dp.expect_all(rules_orders)
def orders_silver():
    """
    Produces a clean, deduplicated stream of orders by:
      1. Filtering out rows that fail any quarantine rule.
      2. Deduplicating on order_id to handle upstream duplicates.
      3. Dropping bronze-layer metadata columns not needed in silver.
    """
    df = (
        spark.readStream.table("orders_bronze_view")
        # Tag each row (mirrors the logic in orders_quarantine).
        .withColumn("is_quarantined", expr(quarantine_rules_orders))
        # Keep only rows that passed all validation rules.
        .filter("is_quarantined = false")
        # Deduplicate: retain the first occurrence of each order_id.
        .dropDuplicates(["order_id"])
    )

    # Remove ingestion/audit columns added during the bronze layer load;
    # they are not relevant for downstream silver consumers.
    df = df.drop(
        "_rescued_data", "_source_file_path", "_source_file_name",
        "_source_modified_time", "_ingested_at", "is_quarantined"
    )
    return df

# COMMAND ----------

# DBTITLE 1,Define product quarantine and silver tables with rules
# ── Quarantine rules ──────────────────────────────────────────────────────────
# Each rule defines a SQL condition that a *valid* product row must satisfy.
# Whitespace-only strings are treated as missing (TRIM check) so that blank
# values don't silently pass as valid product names or categories.
rules_products = {
    "valid_product_id":   "product_id IS NOT NULL",
    "valid_product_name": "product_name IS NOT NULL AND TRIM(product_name) != ''",
    "valid_category":     "categories IS NOT NULL AND TRIM(categories) != ''",
}

# TRUE when a row violates at least one rule — mirrors the orders pattern.
quarantine_rules_products = "NOT ({})".format(" AND ".join(rules_products.values()))


# ── Quarantine table ──────────────────────────────────────────────────────────
@dp.table(name="globalmart_dev.silver.products_quarantine")
def products_quarantine():
    """
    Captures every product row that fails one or more validation rules.
    The `issue_type` column records exactly which checks were violated so
    that bad records can be diagnosed and reprocessed without re-reading
    the full bronze layer.

    Note: the Spark Column expressions use `trim()` rather than the SQL
    TRIM() used in rules_products — both are equivalent but the Column API
    is required inside build_issue_type.
    """
    return (
        spark.readStream.table("products_bronze_view")
        # Tag each row with a boolean indicating whether it is quarantined.
        .withColumn("is_quarantined", expr(quarantine_rules_products))
        # Keep only the rows that failed validation.
        .filter("is_quarantined = true")
        # Derive a human-readable list of the specific issues for each bad row.
        .withColumn("issue_type", build_issue_type({
            "missing_product_id":   col("product_id").isNull(),
            # Treat blank / whitespace-only names the same as NULL.
            "missing_product_name": col("product_name").isNull() | (trim(col("product_name")) == ""),
            # Same blank-check logic applied to category.
            "missing_category":     col("categories").isNull() | (trim(col("categories")) == ""),
        }))
        # The helper column is no longer needed once filtering is done.
        .drop("is_quarantined")
    )


# ── Silver (clean) products table ─────────────────────────────────────────────
@dp.table(
    name="globalmart_dev.silver.products_silver",
    comment="Cleaned products. Sizes normalised. Dates parsed. Category mapped. Weight kept as null — source is ~100% null."
)
# Assert all rules pass for rows that reach the silver table.
# Delta Live Tables will surface any violations as data quality metrics.
@dp.expect_all(rules_products)
def products_silver():
    """
    Produces a clean, deduplicated stream of products by:
      1. Filtering out rows that fail any quarantine rule.
      2. Normalising product_name (trim whitespace).
      3. Parsing and standardising the weight column to kg (or null if absent).
      4. Deduplicating on product_id to handle upstream duplicates.
      5. Dropping bronze-layer metadata columns not needed in silver.
    """
    df = (
        spark.readStream.table("products_bronze_view")
        # Tag each row (mirrors the logic in products_quarantine).
        .withColumn("is_quarantined", expr(quarantine_rules_products))
        # Keep only rows that passed all validation rules.
        .filter("is_quarantined = false")
    )

    # ── Product name cleanup ───────────────────────────────────────────────
    # Strip leading/trailing whitespace so downstream joins and displays are
    # consistent. The NOT NULL + non-empty guarantee is already enforced above.
    df = df.withColumn("product_name", trim(col("product_name")))

    # ── Weight processing ──────────────────────────────────────────────────
    # Source is ~100% null. Guard with isNotNull() before any regexp call
    # so we never crash on a null column. Null weight stays null — do not
    # fill with 0.0 as that implies the product weighs nothing, not unknown.
    df = df.withColumn("weight_value",
        when(col("weight").isNotNull(),
            # Extract the numeric portion of the raw weight string (e.g. "1.5 kg" → 1.5).
            regexp_extract(col("weight"), r"([0-9.]+)", 1).cast(DoubleType())
        ).otherwise(lit(None).cast(DoubleType()))
    ).withColumn("weight_unit",
        when(col("weight").isNotNull(),
            # Extract the unit portion and normalise to lowercase (e.g. "KG" → "kg").
            lower(regexp_extract(col("weight"), r"([a-zA-Z]+)", 1))
        ).otherwise(lit(None))
    ).withColumn("weight",
        # Standardise all weights to kilograms for consistency across the catalogue.
        when(col("weight_unit") == "kg",
            col("weight_value"))                          # already in kg
       .when(col("weight_unit") == "g",
            col("weight_value") / 1000)                  # grams → kg
       .when(col("weight_unit").isin("lb", "lbs", "pound", "pounds"),
            col("weight_value") * 0.453592)               # pounds → kg
       .otherwise(lit(None).cast(DoubleType()))           # unrecognised unit → null
    ).drop("weight_value", "weight_unit")                 # discard intermediate columns

    # ── manufacturer and dimension — keep null, do not fabricate values ────
    # Filling with "Unknown" creates a fake category in Gold aggregations.
    # Null is honest — it means the data was not captured in the source.

    # Deduplicate: retain the first occurrence of each product_id.
    df = df.dropDuplicates(["product_id"])

    # Remove ingestion/audit columns added during the bronze layer load;
    # they are not relevant for downstream silver consumers.
    df = df.drop(
        "_rescued_data", "_source_file_path", "_source_file_name",
        "_source_modified_time", "_ingested_at", "is_quarantined"
    )
    return df

# COMMAND ----------

# DBTITLE 1,Define return quarantine and silver tables with validation
# ── Quarantine rules ──────────────────────────────────────────────────────────
# Each rule defines a SQL condition that a *valid* return row must satisfy.
# Any row failing at least one of these will be routed to the quarantine table.
rules_returns = {
    "valid_order_id":     "order_id IS NOT NULL",
    "valid_return_date":  "return_date IS NOT NULL",
    "valid_refund_amount":"refund_amount IS NOT NULL AND refund_amount > 0",
    "valid_return_reason":"return_reason IS NOT NULL",
}

# Build a single SQL expression that is TRUE when a row violates *any* rule.
quarantine_rules_returns = "NOT ({})".format(" AND ".join(rules_returns.values()))


# ── Quarantine table ──────────────────────────────────────────────────────────
@dp.table(name="globalmart_dev.silver.returns_quarantine")
def returns_quarantine():
    """
    Captures every return row that fails one or more validation rules.
    An `issue_type` column is added to record *which* rules were violated,
    making it easier to diagnose and reprocess bad data downstream.
    
    Note: returns_bronze_view already handles two-schema merge, rescued
    refund_amount recovery, NaN→null conversion, and date parsing.
    """
    return (
        spark.readStream.table("returns_bronze_view")
        # Tag each row with a boolean indicating whether it is quarantined.
        .withColumn("is_quarantined", expr(quarantine_rules_returns))
        # Keep only the rows that failed validation.
        .filter("is_quarantined = true")
        # Derive a human-readable list of the specific issues for each bad row.
        .withColumn("issue_type", build_issue_type({
            "missing_order_id":      col("order_id").isNull(),
            "missing_return_date":   col("return_date").isNull(),
            "missing_refund_amount": col("refund_amount").isNull() | (col("refund_amount") <= 0),
            "missing_return_reason": col("return_reason").isNull(),
        }))
        # The helper column is no longer needed once filtering is done.
        .drop("is_quarantined")
    )


# ── Silver (clean) returns table ──────────────────────────────────────────────
@dp.table(
    name="globalmart_dev.silver.returns_silver",
    comment="Cleaned returns. Two-schema merge resolved. Rescued refund_amount recovered. NaN→null. Dates parsed."
)
# Assert all rules pass for rows that reach the silver table.
# Delta Live Tables will surface any violations as data quality metrics.
@dp.expect_all(rules_returns)
def returns_silver():
    """
    Produces a clean stream of returns by:
      1. Filtering out rows that fail any quarantine rule.
      2. Dropping bronze-layer metadata columns not needed in silver.
    
    All data normalization (schema merge, refund_amount recovery, NaN handling,
    date parsing) is handled upstream in returns_bronze_view.
    """
    df = (
        spark.readStream.table("returns_bronze_view")
        # Tag each row (mirrors the logic in returns_quarantine).
        .withColumn("is_quarantined", expr(quarantine_rules_returns))
        # Keep only rows that passed all validation rules.
        .filter("is_quarantined = false")
    )
    
    # Remove ingestion/audit columns added during the bronze layer load;
    # they are not relevant for downstream silver consumers.
    df = df.drop(
        "_rescued_data", "_source_file_path", "_source_file_name",
        "_source_modified_time", "_ingested_at", "is_quarantined"
    )
    return df

# COMMAND ----------

# DBTITLE 1,Define transaction quarantine and silver tables with validation
# ── Quarantine rules ──────────────────────────────────────────────────────────
# Each rule defines a SQL condition that a *valid* transaction row must satisfy.
# Any row failing at least one of these will be routed to the quarantine table.
rules_transactions = {
    "valid_order_id":       "order_id IS NOT NULL",
    "valid_product_id":     "product_id IS NOT NULL",
    "valid_sales":          "sales IS NOT NULL AND sales > 0",
    "valid_quantity":       "quantity IS NOT NULL AND quantity > 0",
    "valid_discount":       "discount IS NOT NULL AND discount >= 0 AND discount <= 1",
    "valid_payment_type":   "payment_type IS NOT NULL",
    "valid_installments":   "payment_installments IS NOT NULL AND payment_installments > 0",
}

# NOTE: profit IS NOT NULL intentionally excluded from quarantine rules.
# Region 4 (R4) has null profit by design. Quarantining those rows would
# discard an entire legitimate dataset. The Gold layer handles null profit
# as zero-contribution rows in aggregations.
#
# Negative profit IS retained — these are legitimate loss transactions
# resulting from high discounts or promotional pricing.

# Build a single SQL expression that is TRUE when a row violates *any* rule.
quarantine_rules_transactions = "NOT ({})".format(" AND ".join(rules_transactions.values()))


# ── Quarantine table ──────────────────────────────────────────────────────────
@dp.table(name="globalmart_dev.silver.transactions_quarantine")
def transactions_quarantine():
    """
    Captures every transaction row that fails one or more validation rules.
    An `issue_type` column is added to record *which* rules were violated,
    making it easier to diagnose and reprocess bad data downstream.
    
    Note: transactions_bronze_view already handles R3 Order_ID/Product_ID
    recovery from _rescued_data and R4 percentage discount normalization.
    """
    return (
        spark.readStream.table("transactions_bronze_view")
        # Tag each row with a boolean indicating whether it is quarantined.
        .withColumn("is_quarantined", expr(quarantine_rules_transactions))
        # Keep only the rows that failed validation.
        .filter("is_quarantined = true")
        # Derive a human-readable list of the specific issues for each bad row.
        .withColumn("issue_type", build_issue_type({
            "missing_order_id":     col("order_id").isNull(),
            "missing_product_id":   col("product_id").isNull(),
            "invalid_sales":        col("sales").isNull() | (col("sales") <= 0),
            "invalid_quantity":     col("quantity").isNull() | (col("quantity") <= 0),
            "invalid_discount":     (
                col("discount").isNull() |
                (col("discount") < 0) |
                (col("discount") > 1)
            ),
            "missing_payment_type": col("payment_type").isNull(),
            "invalid_installments": (
                col("payment_installments").isNull() |
                (col("payment_installments") <= 0)
            ),
        }))
        # The helper column is no longer needed once filtering is done.
        .drop("is_quarantined")
    )


# ── Silver (clean) transactions table ─────────────────────────────────────────
@dp.table(
    name="globalmart_dev.silver.transactions_silver",
    comment=(
        "Cleaned transactions. "
        "R3 Order_ID/Product_ID recovered from _rescued_data. "
        "R2/R4 Order_id taken directly from Bronze column. "
        "R4 percentage discount normalised to 0–1 decimal. "
        "R4 null profit accepted — Gold handles as zero-contribution. "
        "Negative profit retained — legitimate high-discount loss."
    )
)
# Assert all rules pass for rows that reach the silver table.
# Delta Live Tables will surface any violations as data quality metrics.
@dp.expect_all(rules_transactions)
def transactions_silver():
    """
    Produces a clean stream of transactions by:
      1. Removing any residual "?" placeholders that slipped through Bronze.
      2. Trimming whitespace from payment_type for consistency.
      3. Filtering out rows that fail any quarantine rule.
      4. Dropping bronze-layer metadata columns not needed in silver.
    
    All regional schema reconciliation (R3 rescued columns, R4 discount
    normalization) is handled upstream in transactions_bronze_view.
    """
    df = (
        spark.readStream.table("transactions_bronze_view")

        # ── Safety string cleanup ──────────────────────────────────────────
        # Remove any "?" placeholders that slipped through Bronze.
        # Applied after type casting in the view so these comparisons work
        # against already-typed columns.
        .withColumn("sales",
            when(col("sales").cast("string") == "?", lit(None))
            .otherwise(col("sales"))
        )
        .withColumn("quantity",
            when(col("quantity").cast("string") == "?", lit(None))
            .otherwise(col("quantity"))
        )
        .withColumn("payment_installments",
            when(col("payment_installments").cast("string") == "?", lit(None))
            .otherwise(col("payment_installments"))
        )
        # Trim whitespace from payment_type for downstream join consistency.
        .withColumn("payment_type", trim(col("payment_type")))

        # ── Apply quarantine filter ────────────────────────────────────────
        # Tag each row (mirrors the logic in transactions_quarantine).
        .withColumn("is_quarantined", expr(quarantine_rules_transactions))
        # Keep only rows that passed all validation rules.
        .filter("is_quarantined = false")
    )

    # Remove ingestion/audit columns added during the bronze layer load;
    # they are not relevant for downstream silver consumers.
    df = df.drop(
        "_rescued_data", "_source_file_path", "_source_file_name",
        "_source_modified_time", "_ingested_at", "is_quarantined"
    )
    return df

# COMMAND ----------

# DBTITLE 1,Define vendor quarantine and silver tables with validation
# ── Quarantine rules ──────────────────────────────────────────────────────────
# Each rule defines a SQL condition that a *valid* vendor row must satisfy.
# Whitespace-only strings are treated as missing (TRIM check) so that blank
# values don't silently pass as valid vendor IDs or names.
rules_vendors = {
    "valid_vendor_id":     "vendor_id IS NOT NULL AND TRIM(vendor_id) != ''",
    "valid_vendor_name":   "vendor_name IS NOT NULL AND TRIM(vendor_name) != ''",
    "valid_vendor_id_fmt": "vendor_id RLIKE '^VEN[0-9]+$'",
}

# Build a single SQL expression that is TRUE when a row violates *any* rule.
quarantine_rules_vendors = "NOT ({})".format(" AND ".join(rules_vendors.values()))


# ── Quarantine table ──────────────────────────────────────────────────────────
@dp.table(name="globalmart_dev.silver.vendors_quarantine")
def vendors_quarantine():
    """
    Captures every vendor row that fails one or more validation rules.
    An `issue_type` column is added to record *which* rules were violated,
    making it easier to diagnose and reprocess bad data downstream.
    
    Vendor ID format validation ensures all IDs follow the pattern VEN<number>
    (e.g., VEN001, VEN1234). This prevents downstream join failures caused by
    malformed or inconsistent vendor identifiers.
    """
    return (
        spark.readStream.table("vendors")
        # ── Normalize vendor data ──────────────────────────────────────────
        # Trim whitespace from vendor_id for consistent lookups and joins.
        .withColumn("vendor_id",   trim(col("vendor_id")))
        # Apply title case to vendor_name for display consistency.
        .withColumn("vendor_name", initcap(trim(col("vendor_name"))))
        
        # Tag each row with a boolean indicating whether it is quarantined.
        .withColumn("is_quarantined", expr(quarantine_rules_vendors))
        # Keep only the rows that failed validation.
        .filter("is_quarantined = true")
        # Derive a human-readable list of the specific issues for each bad row.
        .withColumn("issue_type", build_issue_type({
            "missing_vendor_id":   col("vendor_id").isNull() | (trim(col("vendor_id")) == ""),
            "missing_vendor_name": col("vendor_name").isNull() | (trim(col("vendor_name")) == ""),
            "invalid_vendor_id_format": ~col("vendor_id").rlike("^VEN[0-9]+$"),
        }))
        # The helper column is no longer needed once filtering is done.
        .drop("is_quarantined")
    )


# ── Silver (clean) vendors table ──────────────────────────────────────────────
@dp.table(
    name="globalmart_dev.silver.vendors_silver",
    comment="Cleaned vendor reference data. IDs trimmed, names title-cased, format validated."
)
# Assert all rules pass for rows that reach the silver table.
# Delta Live Tables will surface any violations as data quality metrics.
@dp.expect_all(rules_vendors)
def vendors_silver():
    """
    Produces a clean, deduplicated stream of vendors by:
      1. Trimming whitespace from vendor_id for consistent joins.
      2. Applying title case to vendor_name for display consistency.
      3. Filtering out rows that fail any quarantine rule.
      4. Deduplicating on vendor_id to handle upstream duplicates.
      5. Dropping bronze-layer metadata columns not needed in silver.
    
    This ensures downstream Gold queries can reliably join on vendor_id
    without encountering formatting inconsistencies or duplicate entries.
    """
    df = (
        spark.readStream.table("vendors")
        # ── Normalize vendor data ──────────────────────────────────────────
        # Apply the same transformations used in the quarantine table to
        # ensure consistent processing.
        .withColumn("vendor_id",   trim(col("vendor_id")))
        .withColumn("vendor_name", initcap(trim(col("vendor_name"))))
        
        # Tag each row (mirrors the logic in vendors_quarantine).
        .withColumn("is_quarantined", expr(quarantine_rules_vendors))
        # Keep only rows that passed all validation rules.
        .filter("is_quarantined = false")
        # Deduplicate: retain the first occurrence of each vendor_id.
        .dropDuplicates(["vendor_id"])
    )
    
    # Remove ingestion/audit columns added during the bronze layer load;
    # they are not relevant for downstream silver consumers.
    df = df.drop(
        "_rescued_data", "_source_file_path", "_source_file_name",
        "_source_modified_time", "_ingested_at", "is_quarantined"
    )
    return df