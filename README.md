# GlobalMart Data Intelligence Platform

## Overview

The **GlobalMart Data Intelligence Platform** is a comprehensive data lakehouse solution built on Databricks, implementing a medallion architecture (Bronze-Silver-Gold) to transform raw retail data from 6 regional systems into actionable business intelligence.

### Key Features

* **Unified Catalog**: Single Unity Catalog (`globalmart_dev`) for complete data lineage
* **Multi-Region Data**: Consolidates data from 6 regional retail systems (APAC, EMEA, LATAM, NAM)
* **Automated Ingestion**: Auto Loader for incremental, fault-tolerant data processing
* **Schema Evolution**: Handles heterogeneous schemas across regions with rescue columns
* **Data Quality**: Built-in quarantine tables and validation rules
* **AI-Powered Insights**: Multiple use cases leveraging Databricks AI capabilities

---

## Architecture

### Medallion Design

```
┌─────────────────────────────────────────────────────────────┐
│                    globalmart_dev (Catalog)                  │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────┐      ┌──────────┐      ┌──────────┐          │
│  │  BRONZE  │  →   │  SILVER  │  →   │   GOLD   │          │
│  │ Raw Data │      │ Cleansed │      │  Star    │          │
│  │  Schema  │      │  Schema  │      │ Schema   │          │
│  └──────────┘      └──────────┘      └──────────┘          │
│                                                               │
│  - 6 entities      - Standardized    - Dimensions            │
│  - Auto Loader     - Quarantine      - Facts                 │
│  - Rescued data    - Validation      - Aggregates            │
│                    - Deduplication   - Use Cases             │
└─────────────────────────────────────────────────────────────┘
```

### Data Entities

The platform processes 6 core retail entities:

1. **Customers** - Customer profiles and demographics
2. **Orders** - Order transactions and metadata
3. **Transactions** - Payment and transaction details
4. **Returns** - Product returns and refunds
5. **Products** - Product catalog and attributes
6. **Vendors** - Supplier information

---

## Project Structure

```
GlobalMart_DataIntelligence/
│
├── Catalog_setup/
│   └── Catalog_Setup_and_ingestion.ipynb    # Initial setup: catalog, schemas, volume
│
├── bronze/
│   ├── Bronze_ingestion_pipeline.ipynb      # DLT pipeline for raw data ingestion
│   └── Bronze Post Verification.ipynb       # Data quality checks post-ingestion
│
├── silver/
│   ├── silver_ingestion_pipeline.ipynb      # Data cleansing & standardization
│   ├── silver_temporary_views.ipynb         # Reusable temporary views
│   └── silver_post_data_verification.ipynb  # Silver layer validation
│
├── gold/
│   ├── gold_dim_fact_tables.ipynb          # Star schema (dims & facts)
│   ├── aggregation_tables_UC4.py           # Pre-aggregated metrics
│   ├── AI - DATA_Quality_Reporter_UC1.ipynb # UC1: Automated DQ reports
│   ├── Return_fraud_investigater_UC2.ipynb  # UC2: Fraud detection
│   ├── Product_intelligence_assistant_rag_UC3.ipynb # UC3: RAG assistant
│   └── Executive_business_intelligence_UC4.ipynb    # UC4: Executive dashboards
│
├── explorations/
│   └── bronze_analysis.ipynb               # Ad-hoc analysis & exploration
│
├── source_data_files/
│   └── GlobalMart_Retail_Data/             # Raw CSV files (6 regions)
│       ├── Region 1/
│       ├── Region 2/
│       ├── Region 3/
│       ├── Region 4/
│       ├── Region 5/
│       └── Region 6/
│
├── Architecure_Design_Docs/                # Architecture documentation (placeholder)
│
├── GlobalMart_Hackathon_pipeline.yml       # DLT pipeline configuration
│
└── README.md                                # This file
```

---

## Data Flow

### 1. Bronze Layer (Raw Ingestion)

**Purpose**: Ingest raw data from regional systems without transformation

* **Technology**: Databricks Auto Loader (cloud files)
* **Schema**: `globalmart_dev.bronze`
* **Tables**: `customers`, `orders`, `transactions`, `returns`, `products`, `vendors`
* **Features**:
  - Automatic schema inference with type detection
  - Schema evolution via `_rescued_data` column
  - Audit columns: `_source_file_path`, `_source_file_name`, `_source_modified_time`, `_ingested_at`
  - Source region tracking: `source_region` column
  - Idempotent processing (no duplicate ingestion)

**Key Notebook**: `bronze/Bronze_ingestion_pipeline.ipynb`

### 2. Silver Layer (Cleansing & Standardization)

**Purpose**: Cleanse, deduplicate, validate, and standardize data

* **Schema**: `globalmart_dev.silver`
* **Tables**: Clean tables + `_quarantine` tables for rejected records
* **Transformations**:
  - Column name standardization (COALESCE multiple variants)
  - Data type conversions
  - Business rule validation (e.g., positive amounts, valid dates)
  - Deduplication
  - Referential integrity checks
* **Quality Control**: Failed records routed to quarantine tables with failure reasons

**Key Notebooks**: 
- `silver/silver_ingestion_pipeline.ipynb`
- `silver/silver_temporary_views.ipynb`
- `silver/silver_post_data_verification.ipynb`

### 3. Gold Layer (Analytics & Use Cases)

**Purpose**: Star schema for analytics and specialized use cases

* **Schema**: `globalmart_dev.gold`
* **Components**:
  - **Dimensions**: `dim_customers`, `dim_products`, `dim_vendors`, `dim_date`
  - **Facts**: `fact_transactions`, `fact_returns`
  - **Aggregates**: Pre-computed metrics for performance

**Key Notebook**: `gold/gold_dim_fact_tables.ipynb`

---

## Use Cases

### UC1: AI Data Quality Reporter

**Notebook**: `AI - DATA_Quality_Reporter_UC1.ipynb`

**Objective**: Automated data quality monitoring and reporting using AI

**Features**:
- Completeness checks (null rates, missing values)
- Trend analysis (data quality over time)
- Anomaly detection
- Automated alerting for threshold violations
- Visual quality dashboards

---

### UC2: Return Fraud Investigator

**Notebook**: `Return_fraud_investigater_UC2.ipynb`

**Objective**: Detect suspicious return patterns and potential fraud

**Features**:
- High-frequency return pattern detection
- Customer behavior profiling
- Anomaly scoring
- Investigation workflows

---

### UC3: Product Intelligence Assistant (RAG)

**Notebook**: `Product_intelligence_assistant_rag_UC3.ipynb`

**Objective**: Natural language Q&A over product and sales data using Retrieval-Augmented Generation

**Features**:
- Conversational interface for product queries
- Vector search over product descriptions
- Context-aware responses
- Integration with Databricks Foundation Models

---

### UC4: Executive Business Intelligence

**Notebooks**: 
- `Executive_business_intelligence_UC4.ipynb`
- `aggregation_tables_UC4.py`

**Objective**: High-level KPIs and executive dashboards

**Metrics**:
- Revenue trends by region
- Customer lifetime value
- Product performance
- Return rates and reasons
- Vendor performance

---

## Getting Started

### Prerequisites

- Databricks workspace (AWS/Azure/GCP)
- Unity Catalog enabled
- Serverless compute (recommended) or interactive cluster
- Permissions: CREATE CATALOG, CREATE SCHEMA, CREATE TABLE

### Setup Instructions

#### Step 1: Initial Setup

1. Open `Catalog_setup/Catalog_Setup_and_ingestion.ipynb`
2. Run all cells to:
   - Create `globalmart_dev` catalog
   - Create `bronze`, `silver`, `gold` schemas
   - Create Unity Catalog volume for raw data
   - Upload source files to volume
   - Configure RBAC permissions

#### Step 2: Bronze Layer Ingestion

1. Open `bronze/Bronze_ingestion_pipeline.ipynb`
2. This is a DLT (Delta Live Tables) notebook
3. Create a DLT pipeline using `GlobalMart_Hackathon_pipeline.yml` or manually:
   - Add notebook to pipeline
   - Set catalog: `globalmart_dev`
   - Set schema: `bronze`
   - Enable serverless
   - Start pipeline
4. Verify ingestion: Run `bronze/Bronze Post Verification.ipynb`

#### Step 3: Silver Layer Transformation

1. Ensure Bronze pipeline completed successfully
2. Open `silver/silver_ingestion_pipeline.ipynb`
3. Run the notebook or add to DLT pipeline
4. Verify: Run `silver/silver_post_data_verification.ipynb`

#### Step 4: Gold Layer Analytics

1. Open `gold/gold_dim_fact_tables.ipynb`
2. Run to create dimensional model
3. Optionally run `gold/aggregation_tables_UC4.py` for pre-aggregated tables

#### Step 5: Explore Use Cases

Run any of the UC notebooks based on your needs:
- UC1 for data quality monitoring
- UC2 for fraud investigation
- UC3 for conversational product intelligence
- UC4 for executive reporting

---

## Pipeline Configuration

### DLT Pipeline (YAML)

**File**: `GlobalMart_Hackathon_pipeline.yml`

```yaml
name: GlobalMart_Hackathon
catalog: globalmart_dev
schema: bronze
serverless: true
photon: true
continuous: false  # Batch mode
development: false # Production mode

libraries:
  - Bronze_ingestion_pipeline
  - silver_temporary_views
  - silver_ingestion_pipeline
  - gold_dim_fact_tables
```

**To deploy**:
```bash
databricks pipelines create --json @GlobalMart_Hackathon_pipeline.yml
```

---

## Data Quality & Validation

### Bronze Layer Checks

1. **Row Count Validation**: All 6 entities have non-zero records
2. **File Completeness**: All 16 source files (4 regions × 4 file types) ingested
3. **Schema Heterogeneity**: ID and email column variations per region
4. **Rescued Data**: Records with schema mismatches
5. **Regional Coverage**: All 4 regions present in every entity
6. **Audit Metadata**: All audit columns populated
7. **Idempotency**: Row counts stable across re-runs

### Silver Layer Checks

1. **Quarantine Monitoring**: Percentage of rejected records
2. **Completeness**: Null rates for critical fields
3. **Referential Integrity**: Orphaned records detection
4. **Business Rules**: Positive amounts, valid date ranges
5. **Deduplication**: Duplicate record counts
6. **Temporal Analysis**: Data freshness and coverage

**Key Notebooks**:
- `bronze/Bronze Post Verification.ipynb`
- `silver/silver_post_data_verification.ipynb`

---

## Key Technologies

* **Databricks Lakehouse**: Unified analytics platform
* **Unity Catalog**: Centralized governance and lineage
* **Delta Lake**: ACID transactions, time travel, schema evolution
* **Auto Loader**: Incremental file ingestion
* **Delta Live Tables (DLT)**: Declarative ETL framework
* **Photon**: Optimized query engine
* **Serverless Compute**: On-demand, auto-scaling execution
* **MLflow**: Model tracking and registry (UC3)
* **Databricks AI Functions**: AI-powered SQL functions

---

## Best Practices

### Development Workflow

1. **Catalog Isolation**: Use `globalmart_dev` for development, separate catalog for production
2. **Schema Validation**: Always run post-verification notebooks after ingestion
3. **Incremental Processing**: Auto Loader tracks processed files automatically
4. **Quarantine Review**: Regularly investigate quarantine tables
5. **Lineage Tracking**: Use Unity Catalog lineage to trace data flow

### Performance Optimization

1. **Partition Strategy**: Partition large tables by `date` or `region`
2. **Z-Ordering**: Apply OPTIMIZE with Z-ORDER on frequently filtered columns
3. **Caching**: Use Delta caching for frequently accessed tables
4. **Aggregates**: Pre-compute aggregates in Gold layer for dashboard performance

### Data Governance

1. **RBAC**: Use groups (`globalmart_engineering`, etc.) for permission management
2. **Row-Level Security**: Implement if region-specific access needed
3. **PII Masking**: Apply dynamic views for sensitive customer data
4. **Audit Logging**: Leverage Unity Catalog audit logs

---

## Troubleshooting

### Common Issues

**Issue**: Bronze pipeline fails with schema evolution error
- **Solution**: Check `_rescued_data` column, update schema hints in Auto Loader config

**Issue**: High quarantine rates in Silver layer
- **Solution**: Run `silver_post_data_verification.ipynb` to identify validation failures, fix at source

**Issue**: Missing regional data
- **Solution**: Verify volume path, check file upload, review Bronze verification notebook

**Issue**: DLT pipeline stuck
- **Solution**: Check for checkpoint issues, consider resetting pipeline state (development mode only)

---

## Monitoring & Observability

### Metrics to Track

1. **Ingestion Metrics**:
   - Records ingested per run
   - Files processed
   - Processing duration
   - Rescued data count

2. **Data Quality Metrics**:
   - Quarantine rates by entity
   - Null rates for critical fields
   - Duplicate record counts
   - Referential integrity violations

3. **Pipeline Health**:
   - DLT pipeline success rate
   - End-to-end latency
   - Data freshness (ingestion lag)

### Dashboards

- Bronze verification summary
- Silver quality dashboard (UC1)
- Executive KPIs (UC4)

---

## Future Enhancements

* [ ] Real-time streaming ingestion (continuous DLT mode)
* [ ] Machine learning models for demand forecasting
* [ ] Customer segmentation and churn prediction
* [ ] Integration with BI tools (Tableau, Power BI)
* [ ] Automated data quality alerts via webhooks
* [ ] Cross-region performance benchmarking
* [ ] Cost optimization recommendations

---

## Contributors

- **Data Engineering Team**: Pipeline development and optimization
- **Analytics Team**: Use case development and validation
- **Platform Team**: Infrastructure and governance

---

## License

Internal use only - GlobalMart Data Intelligence Platform

---

**Last Updated**: March 27, 2026
**Version**: 1.0