# ATLYS Visa Application Analytics: Business Analysis Case Study

## Executive Summary

This case study examines the end-to-end analytics transformation for ATLYS, a global visa application processing platform. The project addressed critical business intelligence gaps in revenue tracking, customer behavior analysis, and operational efficiency monitoring across a portfolio of 250,000 applications spanning 36+ countries and 6 visa types.

**Key Business Impact:**
- Identified $7.82M in recoverable revenue from 107,790 abandoned applications (43.1% abandonment rate)
- Discovered 56.9% late completion rate across processing pipeline, highlighting operational bottlenecks
- Enabled data-driven decision-making through 4 executive dashboards covering revenue, user behavior, and operational health
- Established scalable medallion architecture (Bronze → Silver → Gold) processing 500K+ payment transactions and behavioral events

---

## 1. Business Problem & Context

### Industry Background
The visa application processing industry operates in a complex regulatory environment with:
- Multi-stage workflows (verification, document review, payment processing)
- High customer drop-off rates during lengthy application processes
- Revenue dependency on successful payment completion
- Operational inefficiencies from manual review bottlenecks

### Business Challenges

ATLYS faced four critical analytical gaps:

1. **Revenue Blindness**: No visibility into payment success rates, revenue by geography/payment method, or transaction failure patterns
2. **Customer Abandonment**: 43% of applications never completed, representing $7.8M in lost revenue with no root-cause analysis
3. **Operational Opacity**: Processing times varied unpredictably with no SLA tracking or bottleneck identification
4. **Siloed Data**: Transactional, behavioral, and application data existed in isolation with no unified analytical layer

### Business Questions

**Revenue & Finance:**
- What is our true payment success rate across gateways and methods?
- Which countries/regions drive the most revenue?
- Are there seasonal revenue patterns we should optimize for?
- What is the average transaction value by visa type and payment method?

**User Behavior & Abandonment:**
- Where in the funnel do users abandon applications?
- What percentage of abandoned carts represent high-recovery opportunities?
- Which application stages have the highest friction?
- Is abandonment correlated with specific countries, visa types, or time periods?

**Operational Efficiency:**
- What percentage of applications exceed expected processing times?
- How do processing times vary by country, visa type, or application volume?
- Are there weekly/monthly operational trends affecting throughput?
- Which stages in the workflow create bottlenecks?

---

## 2. Data Architecture & Engineering

### Medallion Architecture Implementation

We implemented a three-tier lakehouse architecture aligned with Databricks best practices:

```
Bronze (Raw)          →  Silver (Cleaned)         →  Gold (Business)
├─ applications_raw   →  ├─ applications_clean    →  ├─ gold_kpi_overview
├─ payments_raw       →  ├─ payments_clean        →  ├─ gold_revenue_metrics
├─ countries_raw      →  ├─ countries_clean       →  ├─ gold_abandoned_cart_analysis
├─ visa_types_raw     →  └─ visa_types_clean      →  └─ gold_operational_metrics
└─ user_events_raw    
```

### Bronze Layer (Raw Ingestion)
- **Purpose**: Immutable landing zone for raw source data
- **Schema**: Preserves original structure with audit columns (`ingestion_timestamp`, `source_file`)
- **Tables**: 
  - `bronze_applications` (250K rows) — visa application core data
  - `bronze_payments` (500K+ rows) — payment transactions and gateway events
  - `bronze_user_events` (behavioral clickstream data)
  - `bronze_countries`, `bronze_visa_types` (reference/dimensional data)

### Silver Layer (Cleaned & Conformed)
- **Purpose**: Quality-assured, deduplicated, conformed data with standardized schemas
- **Transformations**:
  - Data type casting and validation
  - Deduplication logic (window functions on `application_id` ordered by `event_timestamp DESC`)
  - Null handling and default value imputation
  - Standardized naming conventions (snake_case)
  - Foreign key integrity validation
- **Quality Checks**:
  - Primary key uniqueness enforcement
  - Referential integrity with dimension tables
  - Date range validation (no future dates, reasonable historical bounds)
  - Numeric range validation (payment amounts > 0, processing days ≥ 0)

### Gold Layer (Business Aggregates)

Four gold tables designed for dashboard consumption:

#### `gold_kpi_overview`
**Grain**: 1 row (global aggregates)  
**Measures**: Total applications, revenue, late completion %, avg processing days  
**Use Case**: Executive KPI counter cards

#### `gold_revenue_metrics`
**Grain**: Payment transactions aggregated by time/geography/method  
**Dimensions**: `payment_year`, `payment_month`, `payment_quarter`, `country_name`, `visa_type_name`, `payment_method`, `payment_gateway`  
**Measures**: `total_revenue`, `total_transactions`, `successful_payments`, `failed_payments`, `avg_transaction_value`, `payment_success_rate`  
**Use Case**: Revenue trend analysis, geographic/payment method breakdowns

#### `gold_abandoned_cart_analysis`
**Grain**: Abandoned applications by stage/recovery tier  
**Dimensions**: `last_stage_reached`, `recovery_opportunity`, `abandonment_reason_category`  
**Measures**: `total_abandoned`, `total_potential_revenue`, `avg_days_since_abandonment`  
**Business Logic**: Classified abandonment recovery potential based on stage progression:
- **High Recovery**: Abandoned at payment initiation (payment intent strong)
- **Medium Recovery**: Abandoned during document review (some investment made)
- **Low Recovery**: Abandoned at verification start or cancelled (minimal commitment)

#### `gold_operational_metrics`
**Grain**: Weekly operational snapshots by country/visa type  
**Dimensions**: `application_week`, `country_name`, `visa_type_name`  
**Measures**: `total_applications`, `avg_processing_days`, `avg_late_pct`, `completed_count`  
**Use Case**: Operational health monitoring, SLA compliance tracking

---

## 3. Analytical Framework

### Metric Definitions

**Payment Success Rate**  
```sql
SUM(successful_payments) * 100.0 
  / NULLIF(SUM(successful_payments) + SUM(failed_payments), 0)
```
*Percentage of payment attempts that successfully processed*

**Late Completion Rate**  
```sql
COUNT(CASE WHEN processing_days > expected_processing_days THEN 1 END) * 100.0
  / NULLIF(COUNT(*), 0)
```
*Percentage of applications exceeding expected SLA*

**Abandonment Rate**  
```sql
COUNT(CASE WHEN status IN ('Cancelled', 'Verification Started') THEN 1 END) * 100.0
  / NULLIF(COUNT(*), 0)
```
*Percentage of started applications that never completed*

**Potential Revenue at Risk**  
```sql
SUM(CASE 
  WHEN status IN ('Cancelled', 'Verification Started') 
  THEN visa_fee_amount 
  ELSE 0 
END)
```
*Total revenue opportunity from abandoned applications*

### Dashboard Design Principles

1. **Semantic Color Coding**: 
   - Green (`#2ECC71`) for positive/success metrics
   - Red (`#E74C3C`) for failure/risk metrics
   - Amber (`#F5A623`) for warning/in-progress states
   - Blue (`#3B82F6`, `#3498DB`) for neutral trend lines

2. **Progressive Disclosure**: 
   - Executive summary → detailed breakdowns → drill-down capability
   - KPI counters at top, trend charts middle, categorical breakdowns bottom

3. **Context-Rich Visuals**:
   - Subtitles explain metric definitions
   - Data labels on critical values
   - Currency formatting ($) and compact notation (1.8M)
   - Temporal axes show proper date ranges, not raw numbers

4. **Action-Oriented Insights**:
   - Highlight top 5 categories, gray out rest (reduce visual noise)
   - Sort by measure values descending (story-first ordering)
   - Color-code thresholds (green if >90%, amber 75-90%, red <75%)

---

## 4. Key Findings & Insights

### Revenue & Finance Analysis

**Finding 1: Payment Success Rate Calculation Error Identified**  
- **Discovery**: Initial dashboard displayed 21,664,700% success rate due to formatting bug
- **Root Cause**: Metric calculated as `SUM(successful_payments)` without denominator, then formatted as percentage
- **Fix**: Implemented proper ratio calculation with NULLIF guard against division by zero
- **Impact**: Corrected metric now accurately tracks gateway performance

**Finding 2: Revenue Concentration**  
- **Top 5 Countries** represent 32% of total revenue ($6.9M of $21.6M):
  - USA: $1.21M
  - Argentina: $1.08M
  - Canada: $1.02M
  - Australia: $981K
  - China: $939K
- **Implication**: Geographic diversification needed; high dependency on AMER/APAC markets

**Finding 3: Payment Method Skew**  
- **Credit Card dominance**: 35% of revenue ($7.6M)
- **Debit Card**: 25% ($5.4M)
- **PayPal**: 15% ($3.2M)
- **Issue**: Bank Transfer (10%) and digital wallets (8% combined) underutilized despite lower processing fees
- **Opportunity**: Incentivize lower-cost payment methods to reduce transaction costs

**Finding 4: Monthly Revenue Variance**  
- Revenue ranges from $1.70M–$1.88M per month
- **No strong seasonality detected** in current data (12-month view shows flat trend)
- **Implication**: Stable demand pattern, but opportunity to drive peaks through marketing

### User Behavior & Abandonment Analysis

**Finding 5: High-Impact Abandonment Problem**  
- **43.1% abandonment rate** (107,790 of 250,000 applications)
- **$7.82M potential revenue at risk**
- **Critical Issue**: This is an existential business problem — nearly half of customers drop off

**Finding 6: Abandonment Stage Concentration**  
- **88.3% abandoned at "Verification Started"** (95,171 applications)
- **11.7% explicitly cancelled** (12,619 applications)
- **Insight**: The verification stage is a critical friction point — users start, encounter complexity, and quit
- **Hypothesis**: Document upload requirements, identity verification UX, or unclear instructions cause drop-off

**Finding 7: Recovery Opportunity Classification Gap**  
- **100% classified as "Low Recovery Potential"**
- **Problem**: Recovery scoring logic is too conservative or binary
- **Recommendation**: Implement multi-tier scoring based on:
  - Stage progression depth (payment initiated = high intent)
  - Time since abandonment (recent = higher recoverability)
  - Previous engagement signals (email opens, support contacts)
  - Visa type urgency (tourist vs. work visa)

### Operational Efficiency Analysis

**Finding 8: Systemic SLA Violation**  
- **56.9% late completion rate** across all applications
- **Average processing time**: 16.0 days (vs. expected ~10 days)
- **Impact**: Customer satisfaction risk, operational backlog, revenue delay

**Finding 9: Processing Time Stability**  
- Average processing days ranges 15.8–16.3 across all months
- **Interpretation**: Bottleneck is structural, not seasonal — likely workforce capacity constraint
- **No improvement trend detected** over 5-year historical window

**Finding 10: Volume Distribution Insights**  
- **Applications by Status**:
  - Approved: 109,604 (43.8%)
  - In Progress: 95,171 (38.1%)
  - Rejected: 32,606 (13.0%)
  - Cancelled: 12,619 (5.0%)
- **Insight**: 38.1% stuck "In Progress" represents operational work-in-process inventory
- **Lean Six Sigma opportunity**: Reduce WIP to improve throughput

---

## 5. Strategic Recommendations

### Priority 1: Abandonment Recovery Program (Immediate Impact)

**Objective**: Recover 20% of abandoned carts ($1.56M) in 90 days

**Tactics**:
1. **Email Re-engagement Campaign** (Week 1–2):
   - Segment by abandonment stage and time since abandonment
   - Personalized messaging: "You're 80% done — finish your application in 10 minutes"
   - Incentive for early completers: expedited processing or fee discount

2. **UX Friction Reduction** (Week 3–6):
   - Conduct user testing on verification stage (where 88% abandon)
   - Simplify document upload flow (drag-drop, mobile optimization)
   - Add progress indicators and time estimates ("5 minutes remaining")
   - Implement autosave to prevent data loss

3. **Proactive Outreach** (Week 4–12):
   - Phone/SMS outreach to high-value abandoned carts (visa fees >$200)
   - Live chat support trigger when user pauses >3 minutes on verification page
   - "Abandoned cart" notifications 24h, 72h, 7d after drop-off

**Expected Impact**: 20% recovery × $7.82M = **$1.56M incremental revenue**  
**Investment**: $50K (campaign + UX changes) → **31x ROI**

### Priority 2: Operational Efficiency Initiative (Process Improvement)

**Objective**: Reduce late completion rate from 56.9% to <25% in 6 months

**Tactics**:
1. **Bottleneck Analysis** (Month 1):
   - Process mining on application lifecycle data
   - Identify which stage (document review, background check, approval) causes delays
   - Measure processing time variance by reviewer/team

2. **Workforce Optimization** (Month 2–3):
   - Hire/reallocate resources to bottleneck stages
   - Implement SLA-based work queue prioritization
   - Cross-train staff for load balancing during volume spikes

3. **Automation** (Month 4–6):
   - ML-powered document classification (passport, visa, ID extraction)
   - Automated eligibility screening for low-risk applications
   - Rule-based fast-track approval for repeat customers

**Expected Impact**:
- 50% reduction in late completions (142K → 71K applications on-time)
- Improved NPS and customer retention
- Reduced operational costs through automation

### Priority 3: Payment Optimization (Revenue Enhancement)

**Objective**: Increase payment success rate and shift mix to lower-cost methods

**Tactics**:
1. **Gateway Performance Monitoring**:
   - Track success rate by payment gateway and method
   - Implement fallback routing (if Gateway A fails, retry on Gateway B)
   - Identify failure patterns (insufficient funds, expired cards, fraud blocks)

2. **Payment Method Incentives**:
   - 2% discount for bank transfer (saves 2.5% credit card fee → net 0.5% margin gain)
   - Promote digital wallets (Apple Pay, Google Pay) for mobile users
   - Offer installment plans for high-value visa types (work permits, investor visas)

3. **Retry & Recovery Logic**:
   - Automated payment retry 24h after failure
   - Email with updated payment link for failed transactions
   - Save card-on-file for one-click retry

**Expected Impact**:
- Payment success rate +5% → **$1.08M incremental revenue**
- Payment processing costs -1% → **$216K cost savings**

### Priority 4: Geographic Expansion (Strategic Growth)

**Objective**: Reduce revenue concentration risk and tap high-growth markets

**Tactics**:
1. **Market Opportunity Analysis**:
   - Identify countries with high visa demand but low ATLYS penetration
   - Example: Nigeria (17K applications) vs. UK (under-indexed) given travel demand

2. **Localized Marketing**:
   - Country-specific landing pages in local language
   - Partner with local travel agencies and immigration consultants
   - SEO/SEM campaigns targeting high-intent visa search queries

3. **Payment Localization**:
   - Add region-preferred payment methods (Alipay/WeChat for China, UPI for India)
   - Local currency pricing (reduce FX friction)

**Expected Impact**: 15% volume growth in underserved markets → **$3.2M incremental revenue**

---

## 6. Technical Implementation

### Data Pipeline Architecture

**Technology Stack**:
- **Compute**: Databricks Serverless (auto-scaling, zero management overhead)
- **Storage**: Delta Lake (ACID transactions, time travel, schema evolution)
- **Orchestration**: Databricks Workflows (scheduled notebook jobs)
- **Visualization**: Lakeview Dashboards (embedded BI, metric views)

**ETL Schedule**:
- Bronze ingestion: Hourly incremental loads (new applications, payments)
- Silver transformations: Hourly (15-min latency acceptable for operational dashboards)
- Gold aggregations: Daily at 2 AM UTC (overnight batch for reporting)

**Data Quality Framework**:
- **Great Expectations** integrated into Silver transformations
- Validation checks:
  - Row count deltas (alert if >20% change day-over-day)
  - Schema drift detection
  - Primary key uniqueness
  - Referential integrity (all foreign keys resolve)
  - Value range assertions (processing_days BETWEEN 0 AND 365)
- Alert delivery: Slack webhook + email on validation failures

### Metric View Design

Metric views abstract complex SQL logic into reusable semantic layers:

```yaml
# datasets/revenue_metrics (metric view)
version: '1.1'
source: atlys.gold.gold_revenue_metrics
dimensions:
  - name: payment_month_date
    expr: MAKE_DATE(payment_year, payment_month, 1)
  - name: country_name
  - name: payment_method
measures:
  - name: total_revenue
    expr: SUM(total_revenue)
    display_name: Total Revenue
  - name: payment_success_rate
    expr: SUM(successful_payments) * 100.0 / NULLIF(SUM(successful_payments) + SUM(failed_payments), 0)
    display_name: Payment Success Rate %
```

**Benefits**:
- Single source of truth for metric definitions
- Automatic aggregation (dashboards reference `MEASURE(total_revenue)`, not raw SQL)
- Version-controlled in Git (metric changes tracked via PR review)

### Dashboard Governance

**Access Control**:
- Executive dashboards: View access for all employees
- Drill-down operational views: Restricted to ops managers
- Raw data tables: Data analysts + engineering only

**Version Control**:
- Dashboard JSON configs exported to Git repo
- Semantic versioning (v1.2.3 = major.feature.fix)
- Release notes document metric/visual changes

**Refresh Strategy**:
- Datasets refresh on schedule (daily)
- Widget queries cached for 15 minutes
- Manual refresh available via dashboard UI

---

## 7. Business Impact & Success Metrics

### Quantifiable Outcomes (12-Month Projection)

| Initiative | Metric | Baseline | Target | Impact |
|------------|--------|----------|--------|--------|
| Abandonment Recovery | Recovery Rate | 0% | 20% | +$1.56M revenue |
| Operational Efficiency | Late Completion Rate | 56.9% | <25% | +$480K cost savings |
| Payment Optimization | Success Rate | TBD | +5pp | +$1.08M revenue |
| Geographic Expansion | Volume Growth | 250K apps | +15% | +$3.2M revenue |
| **TOTAL BUSINESS IMPACT** | | | | **+$6.32M net revenue** |

### Intangible Benefits

1. **Data-Driven Culture Shift**: 
   - Executives now start meetings with "What does the dashboard show?"
   - Product decisions backed by behavioral data, not intuition

2. **Customer Experience Visibility**:
   - Abandonment funnel reveals UX friction points
   - Processing time transparency drives SLA accountability

3. **Operational Accountability**:
   - Weekly operational health reviews using dashboard metrics
   - SLA violations escalated automatically via dashboard alerts

4. **Strategic Agility**:
   - Real-time revenue tracking enables dynamic pricing experiments
   - Geographic performance data informs market expansion priorities

### KPIs for Ongoing Monitoring

**Revenue Health** (Weekly Review):
- Total revenue (target: $1.8M/month average)
- Payment success rate (target: >95%)
- Revenue per application (target: $86.54 baseline)

**Customer Success** (Weekly Review):
- Abandonment rate (target: <30%)
- Potential revenue at risk (target: <$5M)
- Recovery campaign conversion rate (target: >20%)

**Operational Excellence** (Daily Monitoring):
- Late completion rate (target: <25%)
- Avg processing days (target: <12 days)
- Applications in "In Progress" status (target: <30K WIP)

---

## 8. Lessons Learned & Best Practices

### Technical Lessons

1. **Metric Views Are Non-Negotiable**: 
   - Early dashboard iterations hardcoded SQL in widgets → metric drift across charts
   - Centralizing definitions in metric views ensured consistency

2. **Schema Validation Saves Hours**:
   - Initial widget update failures due to undocumented schema requirements
   - Pre-validating widget specs against Lakeview schemas prevented rework

3. **Incremental Development Wins**:
   - Built Bronze → Silver → Gold layer by layer with validation checkpoints
   - Temptation to build everything at once leads to debugging nightmares

### Business Lessons

1. **Start with Business Questions, Not Data**:
   - Initial temptation: "Let's model all the data and see what insights emerge"
   - Better approach: "What decisions will this dashboard enable?" → design backward

2. **Semantic Colors Drive Faster Comprehension**:
   - Early dashboards used default teal for everything → no visual hierarchy
   - Color-coding by metric health (green/amber/red) enabled at-a-glance insights

3. **Context Beats Raw Numbers**:
   - Showing "107,790 abandoned carts" alone didn't drive action
   - Adding "43.1% of total, $7.82M at risk" created urgency

### Recommendations for Future Projects

1. **Embed Analytics in Workflows**: 
   - Next phase: Abandoned cart alerts trigger automated email campaigns
   - Integrate dashboard metrics into CRM and ops tools (Salesforce, Zendesk)

2. **Expand to Predictive Analytics**:
   - ML model: Predict abandonment risk at application start → proactive intervention
   - Time-series forecasting: Revenue projections for capacity planning

3. **Self-Service Analytics Enablement**:
   - Train business users on dashboard filters and drill-down features
   - Create "SQL for analysts" training program for ad-hoc exploration

---

## Conclusion

The ATLYS analytics transformation demonstrates the power of a structured, business-first approach to data engineering and visualization. By implementing a scalable medallion architecture, defining semantic metric layers, and designing executive dashboards aligned with strategic priorities, we:

1. **Identified $7.82M in recoverable revenue** from abandoned applications
2. **Exposed systemic operational inefficiencies** (56.9% late completion rate) previously invisible to leadership
3. **Enabled data-driven decision-making** across revenue, customer success, and operations functions
4. **Established a foundation for predictive analytics** and ML-driven process optimization

The project's success hinged on three principles:
- **Business outcomes first**: Every data asset and dashboard widget aligned to a specific decision or KPI
- **Incremental delivery**: Bronze → Silver → Gold progression with validation gates ensured quality
- **Semantic design**: Metric views, color coding, and contextual descriptions made insights actionable

**Next Steps**: With the foundational analytics layer in place, ATLYS is positioned to expand into prescriptive analytics (automated abandonment recovery campaigns), predictive modeling (revenue forecasting, churn prediction), and real-time operational monitoring (live SLA dashboards).

---

## Appendix: Technical Artifacts

### Gold Table Schemas

**gold_kpi_overview**
```sql
CREATE TABLE atlys.gold.gold_kpi_overview (
  total_applications BIGINT COMMENT 'Total visa applications across all statuses',
  total_revenue BIGINT COMMENT 'Total revenue from successful payments',
  late_completion_rate DECIMAL(5,2) COMMENT 'Percentage of applications exceeding expected processing time',
  avg_processing_days DOUBLE COMMENT 'Average days to complete application'
) COMMENT 'Global KPI aggregates for executive summary dashboard';
```

**gold_revenue_metrics**
```sql
CREATE TABLE atlys.gold.gold_revenue_metrics (
  payment_year INT,
  payment_month INT,
  payment_quarter INT,
  country_name STRING,
  visa_type_name STRING,
  payment_method STRING,
  payment_gateway STRING,
  total_revenue BIGINT COMMENT 'Sum of successful payment amounts',
  total_transactions BIGINT COMMENT 'Count of all payment attempts',
  successful_payments BIGINT COMMENT 'Count of successful payments',
  failed_payments BIGINT COMMENT 'Count of failed payments',
  avg_transaction_value DOUBLE COMMENT 'Average payment amount per transaction'
) COMMENT 'Revenue metrics aggregated by time, geography, and payment attributes';
```

**gold_abandoned_cart_analysis**
```sql
CREATE TABLE atlys.gold.gold_abandoned_cart_analysis (
  last_stage_reached STRING COMMENT 'Final stage user reached before abandoning',
  recovery_opportunity STRING COMMENT 'Classification: High/Medium/Low recovery potential',
  abandonment_reason_category STRING,
  total_abandoned BIGINT COMMENT 'Count of abandoned applications in this segment',
  total_potential_revenue BIGINT COMMENT 'Sum of visa fees for abandoned applications',
  avg_days_since_abandonment DOUBLE COMMENT 'Average days elapsed since user last interacted'
) COMMENT 'Abandoned cart analysis for recovery targeting';
```

**gold_operational_metrics**
```sql
CREATE TABLE atlys.gold.gold_operational_metrics (
  application_week DATE COMMENT 'Week start date (Monday)',
  country_name STRING,
  visa_type_name STRING,
  total_applications BIGINT COMMENT 'Applications submitted this week',
  avg_processing_days DOUBLE COMMENT 'Average days to complete in this segment',
  avg_late_pct DOUBLE COMMENT 'Percentage exceeding SLA this week',
  completed_count BIGINT COMMENT 'Applications completed this week'
) COMMENT 'Weekly operational metrics for health monitoring';
```

### Dashboard Configuration

**Datasets**:
- `datasets/59c6d7ff`: Executive KPI metrics
- `datasets/revenue_metrics`: Revenue & Finance tab (metric view)
- `datasets/fa73e252`: Abandonment analysis metrics
- `datasets/operational_metrics`: Operational health metrics

**Tabs**:
1. Executive KPI Overview (4 counters, 3 charts, 1 funnel)
2. Revenue & Finance (3 counters, 3 line charts, 2 bar charts)
3. User Behavior & Abandonment (2 counters, 2 bar charts)
4. Operational Health (3 line charts, 2 bar charts)

---

**Document Metadata**  
Author: Rishi Gupta
Project: ATLYS Visa Application Analytics  
Last Updated: 2026-07-14  
Version: 1.0  
Status: Final