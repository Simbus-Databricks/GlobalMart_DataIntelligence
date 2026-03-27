# Databricks notebook source
# DBTITLE 1,setup connection
from openai import OpenAI
import json
from pyspark.sql.functions import count, sum as _sum, current_timestamp
from pyspark.sql.types import *

# ============================================
# 1. Setup LLM
# ============================================

DATABRICKS_TOKEN = dbutils.notebook.entry_point \
    .getDbutils().notebook().getContext().apiToken().get()

_ws_raw = spark.conf.get("spark.databricks.workspaceUrl")
_ws_url = _ws_raw.replace("https://", "").replace("http://", "").strip("/")

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{_ws_url}/serving-endpoints"
)

MODEL_NAME = "databricks-gpt-oss-20b"

# COMMAND ----------

# DBTITLE 1,Parser
def extract_text(response):
    try:
        content = response.choices[0].message.content

        if isinstance(content, list):
            for item in content:
                if item.get("type") == "text":
                    return item.get("text").strip()

        elif isinstance(content, str):
            parsed = json.loads(content)
            for item in parsed:
                if item.get("type") == "text":
                    return item.get("text").strip()

        return "No valid text found"

    except Exception as e:
        return f"Parsing error: {str(e)}"

# COMMAND ----------

# DBTITLE 1,LLM function
def generate_llm_response(prompt):
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}]
    )
    return extract_text(response)

# COMMAND ----------

# DBTITLE 1,load fact table
from pyspark.sql.functions import expr

# Reload df with proper null handling
df_fact_return = spark.read.table("globalmart_dev.bronze.gold_fact_returns")

# Handle 'null' strings in columns that need to be doubles
cols_to_fix = ["return_amount", "transaction_amount", "return_to_purchase_ratio", "days_to_return"]
for c in cols_to_fix:
    df_fact_return = df_fact_return.withColumn(c, expr(f"try_cast({c} as double)"))

# COMMAND ----------

df_fact_return.describe()

# COMMAND ----------

# DBTITLE 1,load dim customer
dim_customers_df = spark.read.table("globalmart_dev.bronze.gold_dim_customer")
display(dim_customers_df)

# COMMAND ----------

# DBTITLE 1,Join fact and dim
from pyspark.sql.functions import col, when, expr

# Reload tables to avoid path resolution issues
df_fact_return = spark.read.table("globalmart_dev.bronze.gold_fact_returns")
cols_to_fix = ["return_amount", "transaction_amount", "return_to_purchase_ratio", "days_to_return"]
for c in cols_to_fix:
    df_fact_return = df_fact_return.withColumn(c, expr(f"try_cast({c} as double)"))

dim_customers_df = spark.read.table("globalmart_dev.bronze.gold_dim_customer")

# Ensure 'customer_key' is string in both DataFrames
df_fact_return = df_fact_return.withColumn("customer_key", col("customer_key").cast("string"))
dim_customers_df = dim_customers_df.withColumn("customer_key", col("customer_key").cast("string"))

df_final = df_fact_return.join(dim_customers_df, "customer_key")
display(df_final)

# COMMAND ----------

# DBTITLE 1,Rule based scoring as per data
from pyspark.sql.functions import first, count, sum as _sum, avg, max, col

# ============================================
# Aggregate transaction-level data to customer-level
df_aggregated = df_final.groupBy("customer_key").agg(
    count("*").alias("return_count"),
    _sum("return_amount").alias("total_return_amount"),
    avg("return_to_purchase_ratio").alias("avg_return_ratio"),          # Rule 3
    max(col("suspicious_ratio_flag").cast("int")).alias("suspicious_count"),  # Rule 4
    max(col("quick_return_flag").cast("int")).alias("quick_return_count"),    # Rule 5
    first("customer_name").alias("customer_name"),
    first("region").alias("region"),
    first("customer_segment").alias("segment")
)

rows = df_aggregated.collect()

results = []

THRESHOLD = 30   # Score threshold to flag a customer

for row in rows:
    customer_id = row["customer_key"]
    total_returns = int(row["return_count"])
    total_value = float(row["total_return_amount"] or 0)
    avg_return_ratio = float(row["avg_return_ratio"] or 0)
    suspicious_count = int(row["suspicious_count"] or 0)
    quick_return_count = int(row["quick_return_count"] or 0)  # Rule 5
    customer_name = row["customer_name"]
    region = row["region"]
    segment = row["segment"]

    score = 0
    rules = []

    # ------------------------
    # Rule 1: High return frequency
    if total_returns > 5:
        score += 20
        rules.append("high_return_frequency")

    # Rule 2: High return value
    if total_value > 1000:
        score += 25
        rules.append("high_return_value")
    
    # Rule 3: High return ratio
    if avg_return_ratio > 0.5:
        score += 25
        rules.append("high_return_ratio")

    # Rule 4: Suspicious pattern
    if suspicious_count > 0:
        score += 25
        rules.append("suspicious_pattern")
    
    # Rule 5: Quick returns (returned items too quickly)
    if quick_return_count > 0:
        score += 15
        rules.append("quick_return_behavior")

    # ------------------------
    flagged = score >= THRESHOLD

    results.append((
        customer_id,
        customer_name,
        region,
        segment,
        total_returns,
        total_value,
        score,
        ",".join(rules),
        flagged
    ))

# `results` now contains the customer-level return scoring with 5 rules

# COMMAND ----------

# DBTITLE 1,create scored dataframe

df_scored = spark.createDataFrame(
    results,
    ["customer_id", "customer_name", "region", "segment", 
     "total_returns", "total_return_value",
     "anomaly_score", "rules_triggered", "flagged"]
)
df_scored= df_scored.filter(col("customer_id") != -1)

display(df_scored)

# COMMAND ----------

# DBTITLE 1,select flagged customers
flagged_df = df_scored.filter("flagged = true")

# fallback if empty
if flagged_df.count() == 0:
    print("⚠️ No flagged customers → selecting top 3 instead")
    flagged_df = df_scored.orderBy("anomaly_score", ascending=False).limit(3)

display(flagged_df)

# COMMAND ----------

# DBTITLE 1,Prompt Builder
flagged_df = df_scored.orderBy("anomaly_score", ascending=False).limit(3)

display(flagged_df)

# ============================================
# 8. Prompt Builder
# ============================================

def build_prompt(customer_id, total_returns, total_value, rules):
    return f"""
You are a fraud detection analyst.

Analyze the flagged customer based on the following data:

Customer ID: {customer_id}
Total Returns: {total_returns}
Total Return Value: {total_value}
Triggered Rules: {rules}

Instructions:
- Write a concise explanation (3–5 sentences)
- Explain what triggered the anomaly
- Explain why this behavior is risky
- Do NOT suggest investigation steps
- Do NOT assume causes not present in data

Return only a short paragraph.
"""


# COMMAND ----------

# DBTITLE 1,Generate Ai investigation
final_results = []

rows = flagged_df.collect()

for row in rows:
    explanation = generate_llm_response(
        build_prompt(
            row.customer_id,
            row.total_returns,
            row.total_return_value,
            row.rules_triggered
        )
    )

    final_results.append((
        row.customer_id,
        row.customer_name,
        row.region,
        row.segment,
        row.total_returns,
        row.total_return_value,
        row.anomaly_score,
        row.rules_triggered,
        explanation
    ))

# COMMAND ----------

# DBTITLE 1,final output save
schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("total_returns", IntegerType(), True),
    StructField("total_return_value", DoubleType(), True),
    StructField("anomaly_score", IntegerType(), True),
    StructField("rules_triggered", StringType(), True),
    StructField("ai_investigation", StringType(), True)
])

df_output = spark.createDataFrame(final_results, schema) \
    .withColumn("generated_at", current_timestamp())

display(df_output)

# COMMAND ----------

# DBTITLE 1,write to gold
df_output.write.mode("overwrite").saveAsTable(
    "globalmart_dev.gold.flagged_return_customers"
)
display(df_output)