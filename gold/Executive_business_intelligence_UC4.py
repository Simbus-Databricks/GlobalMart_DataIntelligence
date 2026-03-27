# Databricks notebook source
# MAGIC %md
# MAGIC ####Convert aggregated KPIs → into executive insights (AI summaries)

# COMMAND ----------

# UC4: Executive Business Insights Generator
# Objective:
# Convert KPIs into AI-generated executive summaries

# COMMAND ----------

# DBTITLE 1,required tables
df_revenue = spark.read.table("globalmart_dev.gold.mv_revenue_by_region")
df_returns = spark.read.table("globalmart_dev.gold.mv_vendor_returns")
df_slow = spark.read.table("globalmart_dev.gold.mv_inventory_velocity")

# COMMAND ----------

# DBTITLE 1,revenue KPIs
from pyspark.sql.functions import sum as _sum, avg

revenue_kpis = df_revenue.agg(
    _sum("total_revenue").alias("total_revenue"),
    avg("total_revenue").alias("avg_revenue")
).collect()[0].asDict()

# COMMAND ----------

# DBTITLE 1,Vendor return KPIs
return_kpis = df_returns.agg(
    avg("avg_return_ratio").alias("avg_return_rate"),
    _sum("avg_return_ratio").alias("total_return_rate")
).collect()[0].asDict()

# COMMAND ----------

# DBTITLE 1,slow moving inventory KPIs
slow_kpis = df_slow.agg(
    _sum("total_sales").alias("total_slow_inventory"),
    avg("total_sales").alias("avg_inventory")
).collect()[0].asDict()

# COMMAND ----------

# DBTITLE 1,setup connection
 #============================================
# 1. Setup LLM Connection (Secure)
# ============================================

from openai import OpenAI
import json
from pyspark.sql.functions import count, current_timestamp

DATABRICKS_TOKEN = dbutils.notebook.entry_point \
    .getDbutils().notebook().getContext().apiToken().get()

_ws_raw = spark.conf.get("spark.databricks.workspaceUrl")
_ws_url = _ws_raw.replace("https://", "").replace("http://", "").strip("/")

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{_ws_url}/serving-endpoints"
)

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

# ============================================
# 3. LLM Wrapper Function
# ============================================

def generate_llm_response(prompt):
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}]
    )
    return extract_text(response)

# COMMAND ----------

# DBTITLE 1,build prompt
def build_prompt(domain, kpis):
    return f"""
You are a retail business analyst.
 
Based ONLY on the KPI summary provided, generate a concise executive insight.
 
Domain: {domain}
 
KPIs:
{kpis}
 
Instructions:
- Write 4–5 sentences only
- Use simple, clear business language suitable for dashboards
- Base insights strictly on the given KPIs (do NOT assume missing information)
- Avoid generic statements and avoid speculation about causes unless clearly supported by data
- Do NOT repeat raw numbers unnecessarily
 
Focus on:
1. What the KPI indicates (trend or pattern)
2. What business area is impacted (revenue, operations, inventory, vendor performance)
3. What risk or implication this creates
 
Output:
Return a single paragraph only.
"""

# COMMAND ----------

# DBTITLE 1,generate insights
insights = []

# Revenue
insights.append((
    "revenue_performance",
    generate_llm_response(build_prompt("Revenue Performance", revenue_kpis)),
    str(revenue_kpis)
))

# Returns
insights.append((
    "vendor_return_rate",
    generate_llm_response(build_prompt("Vendor Return Rate", return_kpis)),
    str(return_kpis)
))

# Slow-moving inventory
insights.append((
    "slow_moving_inventory",
    generate_llm_response(build_prompt("Slow Moving Inventory", slow_kpis)),
    str(slow_kpis)
))

# COMMAND ----------

# DBTITLE 1,save output
from pyspark.sql.functions import udf, current_timestamp
from pyspark.sql.types import StringType

# Define a function to clean ai_summary
def clean_text(text):
    if text:
        return text.encode('utf-8', 'ignore').decode('utf-8')
    return None

# Register as UDF
clean_text_udf = udf(clean_text, StringType())

# Apply to DataFrame
df_final = spark.createDataFrame(
    insights,
    ["insight_type", "ai_summary", "kpi_data"]
).withColumn("ai_summary", clean_text_udf("ai_summary")) \
 .withColumn("generated_at", current_timestamp())

# Write to table
df_final.write.mode("overwrite").saveAsTable(
    "globalmart_dev.gold.ai_business_insights"
)
display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ai_query() demo

# COMMAND ----------

# DBTITLE 1,Revenue Performance
def ai_query(MODEL_NAME, user_prompt):
    # Aggregate KPIs from mv_inventory_velocity
    df = spark.read.table("globalmart_dev.gold.mv_inventory_velocity")
    from pyspark.sql.functions import sum as _sum, avg

    kpis = df.agg(
        _sum("total_sales").alias("total_sales"),
        avg("total_sales").alias("avg_sales"),
        _sum("order_count").alias("total_orders"),
        avg("order_count").alias("avg_orders")
    ).collect()[0].asDict()

    # Build prompt
    prompt = f"""
You are a retail business analyst.

Based ONLY on the KPI summary provided, generate a concise executive insight.

Domain: Inventory Velocity

KPIs:
{kpis}

User Question:
{user_prompt}

Instructions:
- Write 4–5 sentences only
- Use simple, clear business language suitable for dashboards
- Base insights strictly on the given KPIs (do NOT assume missing information)
- Avoid generic statements and avoid speculation about causes unless clearly supported by data
- Do NOT repeat raw numbers unnecessarily

Focus on:
1. What the KPI indicates (trend or pattern)
2. What business area is impacted (inventory, operations)
3. What risk or implication this creates

Output:
Return a single paragraph only.
"""

    # Generate LLM response
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}]
    )
    result = extract_text(response)
    print(result)
    return result

# COMMAND ----------

# DBTITLE 1,AiQuery 1
user_prompt = "What the KPI indicates (trend or pattern)"
display(ai_query(MODEL_NAME, user_prompt))

# COMMAND ----------

# DBTITLE 1,AiQuery 2
user_prompt = "What business area is impacted (inventory, operations)"
display(ai_query(MODEL_NAME, user_prompt))