# Databricks notebook source
# MAGIC %md
# MAGIC ####RAG implementation
# MAGIC #### UC3: RAG Product Intelligence Assistant
# MAGIC #### Objective:
# MAGIC #### Answer business questions using RAG (Retrieval-Augmented Generation)
# MAGIC #### using product + vendor data

# COMMAND ----------

# DBTITLE 1,setup connection
from openai import OpenAI
import json

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

# DBTITLE 1,Read from aggregated views
# Read from the views created in Cells 5-8
df_slow_products = spark.read.table("globalmart_dev.gold.vw_slow_products_by_region")
df_vendor_summary = spark.read.table("globalmart_dev.gold.vw_vendor_returns_summary")
df_top_categories = spark.read.table("globalmart_dev.gold.vw_top_categories_by_region")
df_high_returns = spark.read.table("globalmart_dev.gold.vw_high_return_products")

# Read dimension tables for additional context
df_products = spark.read.table("globalmart_dev.silver.products_silver")
df_vendors = spark.read.table("globalmart_dev.silver.vendors_silver")

# COMMAND ----------

# DBTITLE 1,Generate Documents from Aggregated Data
# After running Step 2, run this cell to generate better documents
documents = []

# 1. SLOW-MOVING PRODUCTS BY REGION
for row in df_slow_products.collect():
    doc = f"""
    [SLOW-MOVING PRODUCT]
    Product: {row['product_name']}
    Category: {row['category']}
    Region: {row['region_name']}
    Status: {row['movement_status']}
    Order Count: {row['order_count']}
    Total Sales: ${row['total_sales']:.2f}
    
    Analysis: This product is {row['movement_status']} in the {row['region_name']} region with {row['order_count']} orders.
    """
    documents.append(doc.strip())

# 2. VENDOR RETURNS SUMMARY (sorted by return ratio)
vendor_returns = df_vendor_summary.orderBy(df_vendor_summary.avg_return_ratio.desc()).collect()
for rank, row in enumerate(vendor_returns, 1):
    vendor = row['vendor_name'] if row['vendor_name'] else "Unknown Vendor"
    doc = f"""
    [VENDOR PERFORMANCE - RANK #{rank}]
    Vendor: {vendor}
    Total Returns: {row['total_returns']}
    Total Return Value: ${row['total_return_value']:.2f}
    Average Return Ratio: {row['avg_return_ratio']:.2f}
    Rank: #{rank} by return ratio
    
    Analysis: Vendor {vendor} has {row['total_returns']} returns with an average return ratio of {row['avg_return_ratio']:.2f}.
    Higher return ratio indicates POOR vendor performance. This vendor ranks #{rank} in return rates.
    """
    documents.append(doc.strip())

# 3. TOP-SELLING CATEGORIES BY REGION (sorted by revenue)
for row in df_top_categories.orderBy(df_top_categories.total_revenue.desc()).collect():
    doc = f"""
    [CATEGORY SALES BY REGION]
    Region: {row['region_name']}
    Category: {row['category']}
    Total Orders: {row['total_orders']}
    Total Revenue: ${row['total_revenue']:.2f}
    Average Order Value: ${row['avg_order_value']:.2f}
    
    Analysis: In the {row['region_name']} region, the {row['category']} category generated ${row['total_revenue']:.2f} in revenue from {row['total_orders']} orders.
    """
    documents.append(doc.strip())

# 4. HIGH RETURN PRODUCTS (sorted by return value)
high_return_products = df_high_returns.orderBy(df_high_returns.total_return_value.desc()).collect()
for rank, row in enumerate(high_return_products, 1):
    doc = f"""
    [HIGH RETURN PRODUCT - RANK #{rank}]
    Product: {row['product_name']}
    Category: {row['category']}
    Return Count: {row['return_count']}
    Total Return Value: ${row['total_return_value']:.2f}
    Average Return Ratio: {row['avg_return_ratio']:.2f}
    Rank: #{rank} by return value
    
    Analysis: Product {row['product_name']} has high returns with {row['return_count']} returns totaling ${row['total_return_value']:.2f}.
    """
    documents.append(doc.strip())

print(f"Generated {len(documents)} documents for RAG")

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install sentence-transformers faiss-cpu --quiet

# COMMAND ----------

# DBTITLE 1,generate embeddings
from sentence_transformers import SentenceTransformer

embedding_model = SentenceTransformer("all-MiniLM-L6-v2")

embeddings = embedding_model.encode(documents)

# COMMAND ----------

# DBTITLE 1,Load reranker model
# Load cross-encoder for reranking
# Cross-encoders provide better relevance scoring than bi-encoders
from sentence_transformers import CrossEncoder

reranker_model = CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2')

print("✅ Reranker model loaded: cross-encoder/ms-marco-MiniLM-L-6-v2")
print("   This model will rerank retrieved documents for improved relevance.")

# COMMAND ----------

# DBTITLE 1,build faiss index
import faiss
import numpy as np

dimension = embeddings.shape[1]

index = faiss.IndexFlatL2(dimension)
index.add(np.array(embeddings))

# COMMAND ----------

# DBTITLE 1,retrieval function
def retrieve_docs(query, k=5, initial_k=15):
    """
    Retrieve and rerank documents using two-stage retrieval:
    1. Retrieve initial_k candidates using FAISS (fast semantic search)
    2. Rerank using cross-encoder (more accurate relevance scoring)
    3. Return top k documents
    """
    # Stage 1: Retrieve more candidates using bi-encoder (fast)
    query_embedding = embedding_model.encode([query])
    distances, indices = index.search(np.array(query_embedding), initial_k)
    
    candidate_docs = [documents[i] for i in indices[0]]
    
    # Stage 2: Rerank using cross-encoder (accurate)
    pairs = [[query, doc] for doc in candidate_docs]
    rerank_scores = reranker_model.predict(pairs)
    
    # Sort by reranker scores
    ranked_indices = np.argsort(rerank_scores)[::-1]
    
    # Return top k after reranking
    reranked_docs = [candidate_docs[i] for i in ranked_indices[:k]]
    
    return reranked_docs

# COMMAND ----------

# DBTITLE 1,llm strict rules answers
def build_prompt(query, retrieved_docs):
    return f"""
You are a retail analytics assistant.

IMPORTANT INSTRUCTIONS:
- Answer ONLY using the information provided below
- When asked for "highest" or "top", list the top 3-5 items with their exact values
- Include specific numbers (revenue, ratios, counts) from the documents
- For rankings, order from highest to lowest
- If multiple items exist, list them all with their metrics
- If the answer is not found, say: "Data not available in provided documents."

Documents:
{retrieved_docs}

Question:
{query}

Answer (include specific values and rankings):
"""

# COMMAND ----------

# DBTITLE 1,rag query function
def rag_query(user_query):
    docs = retrieve_docs(user_query)

    answer = generate_llm_response(
        build_prompt(user_query, docs)
    )

    return answer, docs

# COMMAND ----------

# DBTITLE 1,test queries
# Comprehensive test queries covering different business questions
queries = [
    # Vendor Performance Analysis
    "Which vendor has the worst return performance?",
    "List all vendors ranked by return ratio from worst to best.",
    "What is Seaborne Ltd's return ratio and total return value?",
    
    # Regional Revenue Analysis
    "Which region generates the most revenue from Women's category?",
    "Compare total revenue between Central and West regions.",
    "What is the average order value for Men's category in the West region?",
    
    # Product Performance
    "Which products are slow-moving in the Central region?",
    "Show me products with the highest return counts.",
    "What product has the highest total return value?",
    
    # Category Insights
    "Which category performs better: Men or Women?",
    "What is the total revenue for each category in the Central region?",
    "How many orders does the Women category have in West region?",
    
    # Comparative Questions
    "Compare Women vs Men category revenue in Central region.",
    "Which has worse returns: Velocity Logistics or Johnsons Logistics?",
    "What's the difference in revenue between top 2 categories in Central region?"
]

results = []

for q in queries:
    answer, docs = rag_query(q)
    
    print(f"\n{'='*80}")
    print(f"Question: {q}")
    print(f"{'='*80}")
    print(f"Answer: {answer}")
    
    results.append((q, answer, str(docs)))

print(f"\n\n{'='*80}")
print(f"Completed {len(queries)} test queries with reranker")
print(f"{'='*80}")

# COMMAND ----------

# DBTITLE 1,log results
from pyspark.sql.functions import current_timestamp

df_log = spark.createDataFrame(
    results,
    ["question", "answer", "retrieved_docs"]
).withColumn("timestamp", current_timestamp())

df_log.write.mode("append").saveAsTable(
    "globalmart_dev.gold.rag_query_history"
)

display(df_log)