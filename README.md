# ATLYS Visa Application Analytics

> **End-to-end analytics transformation** for ATLYS, a global visa application processing platform. This project implements a medallion architecture (Bronze → Silver → Gold) processing 250,000 applications, 500K+ payments, and 2M+ behavioral events to drive data-driven decision-making across revenue, customer success, and operations.

[![Databricks](https://img.shields.io/badge/Platform-Databricks-FF3621?logo=databricks)](https://databricks.com)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta_Lake-003366?logo=delta)](https://delta.io)
[![Status](https://img.shields.io/badge/Status-Production-green)]()

---

## 📊 Project Overview

### Business Impact
- **$7.82M** in recoverable revenue identified from 107,790 abandoned applications (43.1% abandonment rate)
- **56.9%** late completion rate exposed, revealing systemic operational bottlenecks
- **4 executive dashboards** deployed covering revenue, user behavior, and operational health
- **Projected $6.32M** net revenue impact over 12 months from recommended initiatives

### Key Capabilities
✅ Real-time revenue tracking by country, payment method, and visa type  
✅ Abandoned cart analysis with recovery campaign targeting  
✅ Operational SLA monitoring with late completion alerts  
✅ Payment success rate tracking across gateways  
✅ User behavior funnel analysis  

---

## 🏗️ Architecture

### Medallion Lakehouse Design

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────────────┐
│   BRONZE LAYER  │      │   SILVER LAYER  │      │     GOLD LAYER          │
│   (Raw Data)    │─────▶│  (Cleaned Data) │─────▶│  (Business Aggregates)  │
└─────────────────┘      └─────────────────┘      └─────────────────────────┘
                                                              │
                                                              ▼
                                                   ┌──────────────────────┐
                                                   │  METRIC VIEWS        │
                                                   │  (Semantic Layer)    │
                                                   └──────────────────────┘
                                                              │
                                                              ▼
                                                   ┌──────────────────────┐
                                                   │  LAKEVIEW DASHBOARDS │
                                                   │  (Executive BI)      │
                                                   └──────────────────────┘
```

### Technology Stack
| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Compute** | Databricks Serverless | Auto-scaling, zero-ops data processing |
| **Storage** | Delta Lake | ACID transactions, time travel, schema evolution |
| **Orchestration** | Databricks Workflows | Scheduled ETL pipelines |
| **Visualization** | Lakeview Dashboards | Embedded BI with metric views |
| **Quality** | Great Expectations | Data validation and monitoring |
| **Language** | Python, SQL | Data transformation and analysis |

---

## 📁 Repository Structure

```
ATLYS-Visa-Application-Analytics-Transformation-Case-study/
│
├── README.md                                      # This file - project overview & setup
├── Business_Analysis_Case_Study.md                # Comprehensive 25-page case study
├── DATA_DICTIONARY.md                             # Schema definitions & sample data
│
├── Bronze Layer - Raw to Bronze.py                # Raw data ingestion layer
├── Silver Layer - Data Preprocessing.py           # Data cleaning & transformation
└── Gold Layer - Analytics Aggregations.py         # Business metrics & aggregations
```

### File Descriptions

**Documentation Files:**
* `README.md` - Project overview, architecture, setup instructions, and strategic recommendations
* `Business_Analysis_Case_Study.md` - Detailed case study with analysis, findings, and business impact
* `DATA_DICTIONARY.md` - Complete schema definitions, column descriptions, and sample data

**ETL Pipeline Files:**
* `Bronze Layer - Raw to Bronze.py` - Ingests raw application, payment, and user event data into bronze tables
* `Silver Layer - Data Preprocessing.py` - Cleanses data, handles deduplication, and enriches with dimension tables
* `Gold Layer - Analytics Aggregations.py` - Generates business metrics, KPIs, and aggregated views for dashboards

---

## 🚀 Quick Start

### Prerequisites
- Databricks Workspace (AWS, Azure, or GCP)
- Unity Catalog enabled
- Catalog: `atlys` (or customize in notebooks)
- Compute: Serverless or All-Purpose Cluster (DBR 14.3+)

### Setup Instructions

#### 1. Clone Repository to Databricks Workspace
```bash
# Using Databricks CLI
databricks repos create \
  --url https://github.com/Rishi-gupta-data/ATLYS-Visa-Application-Analytics-Transformation-Case-study \
  --path /Repos/rishigupta9711@gmail.com/ATLYS-Visa-Application-Analytics-Transformation-Case-study

# Or via Git integration in Databricks UI
# Workspace → Repos → Add Repo → Enter repo URL
```

#### 2. Create Unity Catalog Schema
```sql
-- Run in Databricks SQL Editor or notebook
CREATE CATALOG IF NOT EXISTS atlys;
USE CATALOG atlys;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

#### 3. Run ETL Pipeline
```python
# Execute notebooks in order:

# Step 1: Bronze Layer - Ingest raw data
# Run: Bronze Layer - Raw to Bronze.py
# Creates: bronze_applications, bronze_payments, bronze_user_events tables

# Step 2: Silver Layer - Clean and transform data
# Run: Silver Layer - Data Preprocessing.py
# Creates: silver_applications, silver_payments, silver_user_events tables

# Step 3: Gold Layer - Generate business metrics
# Run: Gold Layer - Analytics Aggregations.py
# Creates: gold_kpi_overview, gold_revenue_metrics, gold_abandoned_cart, gold_operational_metrics tables
```

#### 4. Build Dashboards
```sql
-- Once gold tables are populated, create Lakeview dashboards in Databricks UI
-- Connect to gold layer tables as data sources
-- Refer to "Dashboard Overview" section below for dashboard specifications
```

---

## 📊 Dashboard Overview

### 1. Executive KPI Overview
**Purpose**: High-level KPIs for leadership  
**Metrics**:
- Total Applications (250K)
- Total Revenue ($21.6M)
- Late Completion Rate (56.9%)
- Avg Processing Days (16.0)

**Visuals**:
- 4 KPI counter cards
- Applications by Status (bar chart with semantic colors)
- Applications Over Time (line chart)
- Processing Time Trend (line chart)

### 2. Revenue & Finance
**Purpose**: Revenue tracking and payment analysis  
**Metrics**:
- Revenue Trend (monthly timeline)
- Payment Success Rate (corrected calculation)
- Revenue by Country (top 36 countries)
- Revenue by Payment Method (6 methods)

**Key Insights**:
- USA leads with $1.21M revenue
- Credit Card dominates at 35% of revenue
- Payment success rate tracking now accurate

### 3. User Behavior & Abandonment
**Purpose**: Customer drop-off and recovery opportunities  
**Metrics**:
- Total Abandoned Carts (107,790 / 43.1%)
- Potential Revenue at Risk ($7.82M)
- Abandonment by Stage (verification = 88% drop-off)
- Recovery Opportunities (segmentation)

**Action Items**:
- Target verification stage UX improvements
- Email re-engagement campaigns
- Proactive chat support triggers

### 4. Operational Health
**Purpose**: SLA monitoring and efficiency tracking  
**Metrics**:
- Weekly Application Volume
- Late Completion % Trend
- Applications by Country
- Applications by Visa Type

**Alert Thresholds**:
- Late completion rate >60% = RED alert
- Processing days >20 = escalate

---

## 🔍 Key Findings

### Revenue Analysis
1. **Geographic Concentration**: Top 5 countries (USA, Argentina, Canada, Australia, China) represent 32% of revenue
2. **Payment Method Skew**: Credit Card (35%), Debit Card (25%), PayPal (15%) — bank transfer underutilized
3. **Flat Seasonality**: No strong monthly revenue variance detected ($1.70M–$1.88M range)

### Abandonment Crisis
1. **43.1% abandonment rate** = existential business problem
2. **88.3% abandon at "Verification Started"** = critical friction point
3. **$7.82M revenue at risk** = immediate recovery opportunity

### Operational Bottlenecks
1. **56.9% late completion rate** = systemic SLA violation
2. **16.0 days avg processing** vs. 10 days expected = 60% over SLA
3. **No improvement trend** over 5-year history = structural issue

---

## 💡 Strategic Recommendations

### Priority 1: Abandonment Recovery ($1.56M impact)
- **Email re-engagement**: Segment by stage, personalized messaging
- **UX friction reduction**: Simplify document upload, add autosave
- **Proactive outreach**: Phone/SMS for high-value carts (>$200)

### Priority 2: Operational Efficiency ($480K savings)
- **Bottleneck analysis**: Process mining to identify delay stages
- **Workforce optimization**: Reallocate resources to bottleneck stages
- **Automation**: ML document classification, eligibility screening

### Priority 3: Payment Optimization ($1.08M + $216K savings)
- **Gateway monitoring**: Track success rate by gateway
- **Payment incentives**: 2% discount for bank transfer
- **Retry logic**: Automated retry 24h after failure

### Priority 4: Geographic Expansion ($3.2M growth)
- **Market analysis**: Identify high-demand, low-penetration countries
- **Localized marketing**: Country-specific landing pages
- **Payment localization**: Add region-preferred methods (Alipay, UPI)

**Total Projected Impact**: **$6.32M net revenue** over 12 months

---

## 🧪 Data Quality & Testing

### Validation Framework
```python
# Example: Great Expectations validation in Silver layer
import great_expectations as gx

# Primary key uniqueness
expect_column_values_to_be_unique(column='application_id')

# Referential integrity
expect_column_values_to_be_in_set(
    column='country_id', 
    value_set=bronze_countries.select('country_id').distinct()
)

# Range validation
expect_column_values_to_be_between(
    column='processing_days', 
    min_value=0, 
    max_value=365
)
```

### Unit Tests
```bash
# Run pytest suite
pytest tests/ -v

# Coverage report
pytest tests/ --cov=notebooks --cov-report=html
```

---

## 📅 ETL Schedule

| Layer | Frequency | Schedule | Latency |
|-------|-----------|----------|---------|
| Bronze | Hourly | :00 every hour | Near real-time |
| Silver | Hourly | :15 every hour | 15 minutes |
| Gold | Daily | 2:00 AM UTC | Overnight batch |
| Dashboards | On-demand | Manual refresh | < 1 minute |

---

## 🤝 Contributing

### Development Workflow
1. Create feature branch: `git checkout -b feature/new-metric`
2. Make changes in notebooks or dashboards
3. Run unit tests: `pytest tests/`
4. Commit with descriptive message: `git commit -m "Add recovery scoring logic"`
5. Push and create PR: `git push origin feature/new-metric`

### Code Standards
- Python: PEP 8 (use `black` formatter)
- SQL: Lowercase keywords, snake_case identifiers
- Notebooks: Clear markdown documentation in each cell
- Dashboards: Semantic colors, consistent naming

### Testing Requirements
- All new transformations require unit tests
- Silver layer: validate deduplication, FK integrity, range checks
- Gold layer: validate aggregation logic, metric formulas

---

## 📚 Documentation

| Document | Description | Audience |
|----------|-------------|----------|
| [README.md](README.md) | Project overview & setup | All users |
| [Business_Analysis_Case_Study.md](Business_Analysis_Case_Study.md) | 25-page comprehensive case study | Executives, stakeholders |
| [DATA_DICTIONARY.md](DATA_DICTIONARY.md) | Schema definitions & sample data | Developers, analysts |
| ETL Pipeline Files | Inline documentation in Python notebooks | Engineers |

---

## 🔒 Security & Access Control

### Unity Catalog Permissions
```sql
-- Executive dashboards: View access for all employees
GRANT SELECT ON SCHEMA atlys.gold TO `all-employees-group`;

-- Raw data tables: Data analysts + engineering only
GRANT SELECT ON SCHEMA atlys.bronze TO `data-engineering-group`;
GRANT SELECT ON SCHEMA atlys.silver TO `data-analyst-group`;

-- Modify permissions: Engineering only
GRANT MODIFY ON SCHEMA atlys.bronze TO `data-engineering-group`;
GRANT MODIFY ON SCHEMA atlys.silver TO `data-engineering-group`;
GRANT MODIFY ON SCHEMA atlys.gold TO `data-engineering-group`;
```

### Row-Level Security (Future Enhancement)
```sql
-- Example: Restrict country visibility by user role
CREATE FUNCTION atlys.gold.country_filter()
RETURN IF(is_member('regional-manager-apac'), country_name IN ('Australia', 'China', 'Japan'), TRUE);

ALTER TABLE atlys.gold.gold_revenue_metrics SET ROW FILTER atlys.gold.country_filter ON (country_name);
```

---

## 🐛 Troubleshooting

### Common Issues

**Issue**: Dashboard widgets show "Query failed"  
**Solution**: Check dataset refresh status. Manually refresh via Dashboard UI or wait for scheduled refresh.

**Issue**: Metric view returns NULL for payment_success_rate  
**Solution**: Verify both `successful_payments` and `failed_payments` columns exist in source table. Check NULLIF logic.

**Issue**: Late completion rate shows 0%  
**Solution**: Ensure `expected_processing_days` is populated in `silver_applications`. Check FK join to `visa_types`.

**Issue**: Bronze ingestion duplicate records  
**Solution**: Check deduplication logic in Silver layer. Verify window function ordering by `event_timestamp DESC`.

---

## 📈 Roadmap

### Q3 2024
- ✅ Bronze/Silver/Gold pipeline deployed
- ✅ 4 executive dashboards live
- ✅ Metric views implemented
- 🔲 Email re-engagement campaign (abandonment recovery)

### Q4 2024
- 🔲 ML abandonment prediction model
- 🔲 Real-time SLA alerting (Slack integration)
- 🔲 Payment gateway fallback routing
- 🔲 Self-service analytics training program

### Q1 2025
- 🔲 Predictive revenue forecasting
- 🔲 Customer LTV modeling
- 🔲 Automated CRM integration (Salesforce sync)
- 🔲 Advanced recovery scoring (High/Medium/Low tiers)

---

## 📞 Support & Contact

**Project Owner**: Rishi Gupta  
**Email**: rishigupta9711@gmail.com  
**Workspace**: Databricks ATLYS Analytics  

### Support Channels
- **Technical Issues**: Create GitHub issue with `[BUG]` prefix
- **Feature Requests**: Create GitHub issue with `[FEATURE]` prefix
- **Data Questions**: Slack #data-analytics channel
- **Dashboard Access**: Email project owner

---

## 🙏 Acknowledgments

Special thanks to:
- **Data Engineering Team** for medallion architecture implementation
- **Business Stakeholders** for defining KPIs and validation
- **Databricks** for platform support and best practices guidance
- **Community** for open-source tools (Great Expectations, Delta Lake)

---

**Last Updated**: July 14, 2024  
**Version**: 1.0  
**Status**: Production
