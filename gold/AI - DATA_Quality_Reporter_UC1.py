# Databricks notebook source
# DBTITLE 1,setup connection
from openai import OpenAI
import json
from pyspark.sql.functions import count, current_timestamp

# ============================================
# 1. Setup LLM Connection (Secure)
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

# MAGIC %md
# MAGIC ####Use case 1: AI Data Quality Reporter

# COMMAND ----------

# DBTITLE 1,Response parser

MODEL_NAME = "databricks-gpt-oss-20b"

# ============================================
# 2. Response Parser (MANDATORY)
# ============================================

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

# DBTITLE 1,LLM wrapper function
def generate_llm_response(prompt):
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}]
    )
    return extract_text(response)

# COMMAND ----------

# DBTITLE 1,Load your data
df_customer_quarantine = spark.read.table("globalmart_dev.silver.customers_quarantine")
df_orders_quarantine = spark.read.table("globalmart_dev.silver.orders_quarantine")

# COMMAND ----------

df_customer_quarantine.printSchema()


# COMMAND ----------

# DBTITLE 1,rename columns
from pyspark.sql.functions import when, col, lit

df_q = df_customer_quarantine


df_q = df_q.withColumn(
    "field",
    when(col("issue_type").contains("email"), "customer_email")
    .when(col("issue_type").contains("segment"), "segment")
    .when(col("issue_type").contains("postal"), "postal_code")
    .otherwise("unknown")
)

df_q = df_q.withColumn("entity", lit("customers"))

# COMMAND ----------

# DBTITLE 1,grouping
from pyspark.sql.functions import count

grouped_df = df_q.groupBy("entity", "field", "issue_type") \
    .agg(count("*").alias("count"))

# COMMAND ----------

grouped_df.display()

# COMMAND ----------

# DBTITLE 1,Prompt builder
def build_prompt(entity, field, issue, count):
    return f"""
You are a financial data quality audit assistant.

Analyze the following grouped data quality issue and generate a concise business explanation.

Context:
- Entity: {entity}
- Field: {field}
- Issue Type: {issue}
- Rejected Records: {count}

Instructions:
- Write a single paragraph (4–6 sentences)
- Do NOT use bullet points
- Do NOT repeat content
- Do NOT invent financial numbers

Your explanation MUST include:

1. What the problem is and what value or pattern triggered the rejection  
   (clearly mention the field and issue type)

2. Why these records cannot be accepted into the analytics layer  
   (focus on joins, aggregations, and data integrity)

3. What business report, audit figure, or operational decision is at risk  
   (explain real business impact in 3–4 sentences)

Keep the explanation clear, specific, and business-focused.
"""

# COMMAND ----------

# DBTITLE 1,Query your data to generate Ai EXPLANATION
rows = grouped_df.collect()    ####replace with silver quarantine dataframe of globalmart
results = []

for row in rows:
    entity = row["entity"]
    field = row["field"]
    issue = row["issue_type"]
    count_val = row["count"]

    explanation = generate_llm_response(
        build_prompt(entity, field, issue, count_val)
    )

    results.append((entity, field, issue, count_val, explanation))

# COMMAND ----------

# DBTITLE 1,Output dataframe
df_result = spark.createDataFrame(
    results,
    ["entity", "field", "issue_type", "count", "ai_explanation"]
).withColumn("generated_at", current_timestamp())

# COMMAND ----------

df_result.display()

# COMMAND ----------

# DBTITLE 1,write to delta table
df_result.write.mode("overwrite").saveAsTable(
    "globalmart_dev.gold.dq_audit_report"
)

# COMMAND ----------

# DBTITLE 1,validate

print("\n🔍 Preview  AI Data quality report:")
display(df_result)

# COMMAND ----------

# DBTITLE 1,Cell 16
from pyspark.sql.functions import when, col, lit, expr
 
df_q = df_orders_quarantine
 
# -----------------------------
# ISSUE COLUMN
# -----------------------------
df_q = df_q.withColumn(
    "issue",
    when(col("order_id").isNull(), "missing_order_id")
    .when(col("customer_id").isNull(), "missing_customer_id")
    .when(col("vendor_id").isNull(), "missing_vendor_id")
    .when(col("order_purchase_date").isNull(), "missing_purchase_date")
    .when(
        (col("order_delivered_customer_date").isNotNull()) & 
        (expr("try_cast(order_purchase_date AS TIMESTAMP)").isNotNull()) &
        (col("order_delivered_customer_date") < expr("try_cast(order_purchase_date AS TIMESTAMP)")),
        "invalid_delivery_date"
    )
    .when(col("order_status").isNull(), "missing_order_status")
    .otherwise("other_issue")
)
 
# -----------------------------
# FIELD COLUMN
# -----------------------------
df_q = df_q.withColumn(
   "field",
   when(col("order_id").isNull(), "order_id")
   .when(col("customer_id").isNull(), "customer_id")
   .when(col("vendor_id").isNull(), "vendor_id")
   .when(col("order_purchase_date").isNull(), "order_purchase_date")
   .when(col("order_delivered_customer_date") < expr("try_cast(order_purchase_date AS TIMESTAMP)"), "order_delivered_customer_date")
   .when(col("order_estimated_delivery_date").isNull(), "order_estimated_delivery_date")
    .when(col("order_delivered_carrier_date").isNull(), "order_delivered_carrier_date")
    .when(col("order_delivered_customer_date").isNull(), "order_delivered_customer_date")
    .when(col("order_approved_at").isNull(), "order_approved_at")
   .otherwise("unknown")
)
 
# -----------------------------
# ENTITY COLUMN
# -----------------------------
df_q = df_q.withColumn("entity", lit("orders"))

# COMMAND ----------

from pyspark.sql.functions import count

grouped_df = df_q.groupBy("entity", "field", "issue_type") \
    .agg(count("*").alias("count"))

# COMMAND ----------

def build_prompt(entity, field, issue, count):
    return f"""
You are a financial data quality audit assistant.

Analyze the following grouped data quality issue and generate a concise business explanation.

Context:
- Entity: {entity}
- Field: {field}
- Issue Type: {issue}
- Rejected Records: {count}

Instructions:
- Write a single paragraph (4–6 sentences)
- Do NOT use bullet points
- Do NOT repeat content
- Do NOT invent financial numbers

Your explanation MUST include:

1. What the problem is and what value or pattern triggered the rejection  
   (clearly mention the field and issue type)

2. Why these records cannot be accepted into the analytics layer  
   (focus on joins, aggregations, and data integrity)

3. What business report, audit figure, or operational decision is at risk  
   (explain real business impact in 3–4 sentences)

Keep the explanation clear, specific, and business-focused.
"""

# COMMAND ----------

rows = grouped_df.collect()    ####replace with silver quarantine dataframe of globalmart
results = []

for row in rows:
    entity = row["entity"]
    field = row["field"]
    issue = row["issue_type"]
    count_val = row["count"]

    explanation = generate_llm_response(
        build_prompt(entity, field, issue, count_val)
    )

    results.append((entity, field, issue, count_val, explanation))

# COMMAND ----------

df_result = spark.createDataFrame(
    results,
    ["entity", "field", "issue_type", "count", "ai_explanation"]
).withColumn("generated_at", current_timestamp())

# COMMAND ----------

df_result.write.mode("overwrite").saveAsTable(
    "globalmart_dev.gold.dq_audit_report"
)

# COMMAND ----------


print("\n🔍 Preview  AI Data quality report:")
display(df_result)