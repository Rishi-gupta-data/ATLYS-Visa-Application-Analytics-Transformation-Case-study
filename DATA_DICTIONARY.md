# ATLYS Data Dictionary & Sample Data

## Overview
This document provides detailed schema definitions and sample data for all tables in the ATLYS visa application analytics project, organized by medallion layer (Bronze → Silver → Gold).

---

## Bronze Layer (Raw Data)

### bronze_applications
**Purpose**: Raw visa application data from source systems  
**Row Count**: 250,000  
**Grain**: One row per application

#### Schema
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| application_id | BIGINT | Unique application identifier | 1001 |
| user_id | BIGINT | User account ID | 5001 |
| country_id | INT | Destination country FK | 1 |
| visa_type_id | INT | Visa type FK | 2 |
| application_date | DATE | Application submission date | 2024-01-15 |
| status | STRING | Current status | Approved |
| processing_days | INT | Days to process | 14 |
| expected_processing_days | INT | Expected SLA days | 10 |
| visa_fee_amount | DECIMAL(10,2) | Application fee | 100.00 |
| ingestion_timestamp | TIMESTAMP | Record loaded at | 2024-01-15 08:30:00 |

#### Sample Data
```sql
SELECT * FROM atlys.bronze.bronze_applications LIMIT 5;
```

| application_id | user_id | country_id | visa_type_id | application_date | status | processing_days | expected_processing_days | visa_fee_amount |
|----------------|---------|------------|--------------|------------------|--------|-----------------|-------------------------|-----------------|
| 1001 | 5001 | 1 | 2 | 2024-01-15 | Approved | 14 | 10 | 100.00 |
| 1002 | 5002 | 3 | 1 | 2024-01-16 | Rejected | 18 | 10 | 150.00 |
| 1003 | 5003 | 2 | 3 | 2024-01-17 | Pending | 8 | 10 | 75.00 |
| 1004 | 5004 | 1 | 2 | 2024-01-18 | Approved | 12 | 10 | 100.00 |
| 1005 | 5005 | 5 | 4 | 2024-01-19 | Cancelled | 5 | 10 | 200.00 |

---

### bronze_payments
**Purpose**: Raw payment transaction records from payment gateways  
**Row Count**: 500,000+  
**Grain**: One row per payment attempt

#### Schema
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| payment_id | BIGINT | Unique payment identifier | 2001 |
| application_id | BIGINT | Application FK | 1001 |
| payment_method | STRING | Payment type | Credit Card |
| payment_gateway | STRING | Gateway used | Stripe |
| payment_amount | DECIMAL(10,2) | Transaction amount | 100.00 |
| payment_status | STRING | Success/Failed | Success |
| payment_date | TIMESTAMP | Transaction timestamp | 2024-01-15 10:30:00 |
| gateway_transaction_id | STRING | Gateway reference | txn_abc123 |
| ingestion_timestamp | TIMESTAMP | Record loaded at | 2024-01-15 10:31:00 |

#### Sample Data
```sql
SELECT * FROM atlys.bronze.bronze_payments LIMIT 5;
```

| payment_id | application_id | payment_method | payment_gateway | payment_amount | payment_status | payment_date |
|------------|----------------|----------------|-----------------|----------------|----------------|--------------|
| 2001 | 1001 | Credit Card | Stripe | 100.00 | Success | 2024-01-15 10:30:00 |
| 2002 | 1002 | PayPal | PayPal | 150.00 | Success | 2024-01-16 11:15:00 |
| 2003 | 1003 | Debit Card | Stripe | 75.00 | Failed | 2024-01-17 09:45:00 |
| 2004 | 1003 | Debit Card | Stripe | 75.00 | Success | 2024-01-17 10:00:00 |
| 2005 | 1004 | Bank Transfer | Direct | 100.00 | Success | 2024-01-18 14:20:00 |

---

### bronze_user_events
**Purpose**: Behavioral clickstream data from application portal  
**Row Count**: 2,000,000+  
**Grain**: One row per user interaction event

#### Schema
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| event_id | BIGINT | Unique event identifier | 3001 |
| user_id | BIGINT | User account ID | 5001 |
| application_id | BIGINT | Application context | 1001 |
| event_type | STRING | Action type | page_view, form_submit |
| event_timestamp | TIMESTAMP | Event occurred at | 2024-01-15 09:00:00 |
| page_url | STRING | Current page | /verification |
| session_id | STRING | Session identifier | sess_xyz789 |
| ingestion_timestamp | TIMESTAMP | Record loaded at | 2024-01-15 09:01:00 |

#### Sample Data
```sql
SELECT * FROM atlys.bronze.bronze_user_events LIMIT 5;
```

| event_id | user_id | application_id | event_type | event_timestamp | page_url |
|----------|---------|----------------|------------|-----------------|----------|
| 3001 | 5001 | 1001 | page_view | 2024-01-15 09:00:00 | /application/start |
| 3002 | 5001 | 1001 | form_submit | 2024-01-15 09:05:00 | /personal-details |
| 3003 | 5001 | 1001 | page_view | 2024-01-15 09:06:00 | /verification |
| 3004 | 5001 | 1001 | document_upload | 2024-01-15 09:15:00 | /verification |
| 3005 | 5001 | 1001 | page_view | 2024-01-15 09:20:00 | /payment |

---

### bronze_countries
**Purpose**: Reference data for destination countries  
**Row Count**: 36  
**Grain**: One row per country

#### Schema
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| country_id | INT | Country identifier | 1 |
| country_code | STRING | ISO 2-letter code | US |
| country_name | STRING | Full country name | USA |
| region | STRING | Geographic region | North America |
| ingestion_timestamp | TIMESTAMP | Record loaded at | 2024-01-01 00:00:00 |

#### Sample Data
```sql
SELECT * FROM atlys.bronze.bronze_countries LIMIT 10;
```

| country_id | country_code | country_name | region |
|------------|--------------|--------------|--------|
| 1 | US | USA | North America |
| 2 | GB | United Kingdom | Europe |
| 3 | AU | Australia | Oceania |
| 4 | CA | Canada | North America |
| 5 | JP | Japan | Asia |
| 6 | DE | Germany | Europe |
| 7 | FR | France | Europe |
| 8 | CN | China | Asia |
| 9 | IN | India | Asia |
| 10 | BR | Brazil | South America |

---

### bronze_visa_types
**Purpose**: Reference data for visa categories  
**Row Count**: 6  
**Grain**: One row per visa type

#### Schema
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| visa_type_id | INT | Visa type identifier | 1 |
| visa_type_code | STRING | Short code | TOUR |
| visa_type_name | STRING | Full name | Tourist Visa |
| base_fee | DECIMAL(10,2) | Standard fee | 100.00 |
| expected_processing_days | INT | SLA days | 10 |
| ingestion_timestamp | TIMESTAMP | Record loaded at | 2024-01-01 00:00:00 |

#### Sample Data
```sql
SELECT * FROM atlys.bronze.bronze_visa_types;
```

| visa_type_id | visa_type_code | visa_type_name | base_fee | expected_processing_days |
|--------------|----------------|----------------|----------|-------------------------|
| 1 | TOUR | Tourist Visa | 100.00 | 10 |
| 2 | WORK | Work Permit | 200.00 | 15 |
| 3 | STUD | Student Visa | 150.00 | 12 |
| 4 | BUSI | Business Visa | 175.00 | 8 |
| 5 | FAMI | Family Reunion | 125.00 | 14 |
| 6 | TRAN | Transit Visa | 50.00 | 5 |

---

## Silver Layer (Cleaned & Conformed)

### silver_applications
**Purpose**: Deduplicated, validated, standardized application data  
**Row Count**: 250,000  
**Grain**: One row per application (unique on application_id)

#### Schema
| Column | Type | Description | Cleaning Rule |
|--------|------|-------------|---------------|
| application_id | BIGINT | Unique application ID | PK, NOT NULL |
| user_id | BIGINT | User account ID | FK validated |
| country_id | INT | Destination country FK | FK validated |
| visa_type_id | INT | Visa type FK | FK validated |
| application_date | DATE | Submission date | Range validated (2020-2026) |
| status | STRING | Current status | Enum validated |
| status_category | STRING | Grouped status | Derived field |
| processing_days | INT | Actual days to process | Validated ≥ 0 |
| expected_processing_days | INT | Expected SLA days | From visa_types |
| is_late | BOOLEAN | Exceeded SLA? | processing_days > expected |
| visa_fee_amount | DECIMAL(10,2) | Application fee | Validated > 0 |
| last_updated | TIMESTAMP | Record last modified | Audit column |

#### Sample Data
```sql
SELECT * FROM atlys.silver.silver_applications LIMIT 5;
```

| application_id | user_id | country_id | visa_type_id | application_date | status | status_category | processing_days | is_late |
|----------------|---------|------------|--------------|------------------|--------|-----------------|-----------------|---------|
| 1001 | 5001 | 1 | 2 | 2024-01-15 | Approved | Success | 14 | TRUE |
| 1002 | 5002 | 3 | 1 | 2024-01-16 | Rejected | Failure | 18 | TRUE |
| 1003 | 5003 | 2 | 3 | 2024-01-17 | Pending | In Progress | 8 | FALSE |
| 1004 | 5004 | 1 | 2 | 2024-01-18 | Approved | Success | 12 | TRUE |
| 1005 | 5005 | 5 | 4 | 2024-01-19 | Cancelled | Other | 5 | FALSE |

---

### silver_payments
**Purpose**: Deduplicated payment transactions with retry tracking  
**Row Count**: 500,000+  
**Grain**: One row per payment attempt (unique on payment_id)

#### Schema
| Column | Type | Description | Cleaning Rule |
|--------|------|-------------|---------------|
| payment_id | BIGINT | Unique payment ID | PK, NOT NULL |
| application_id | BIGINT | Application FK | FK validated |
| payment_method | STRING | Payment type | Standardized labels |
| payment_gateway | STRING | Gateway used | Standardized labels |
| payment_amount | DECIMAL(10,2) | Transaction amount | Validated > 0 |
| payment_status | STRING | Success/Failed | Enum validated |
| payment_date | TIMESTAMP | Transaction timestamp | Range validated |
| payment_year | INT | Derived year | YEAR(payment_date) |
| payment_month | INT | Derived month | MONTH(payment_date) |
| payment_quarter | INT | Derived quarter | QUARTER(payment_date) |
| is_retry | BOOLEAN | Retry attempt? | Window function logic |
| last_updated | TIMESTAMP | Record last modified | Audit column |

#### Sample Data
```sql
SELECT * FROM atlys.silver.silver_payments LIMIT 5;
```

| payment_id | application_id | payment_method | payment_gateway | payment_amount | payment_status | payment_year | payment_month | is_retry |
|------------|----------------|----------------|-----------------|----------------|----------------|--------------|---------------|----------|
| 2001 | 1001 | Credit Card | Stripe | 100.00 | Success | 2024 | 1 | FALSE |
| 2002 | 1002 | PayPal | PayPal | 150.00 | Success | 2024 | 1 | FALSE |
| 2003 | 1003 | Debit Card | Stripe | 75.00 | Failed | 2024 | 1 | FALSE |
| 2004 | 1003 | Debit Card | Stripe | 75.00 | Success | 2024 | 1 | TRUE |
| 2005 | 1004 | Bank Transfer | Direct | 100.00 | Success | 2024 | 1 | FALSE |

---

## Gold Layer (Business Aggregates)

### gold_kpi_overview
**Purpose**: Global KPI metrics for executive summary dashboard  
**Row Count**: 1  
**Grain**: Single row with global aggregates

#### Schema
| Column | Type | Description | Business Logic |
|--------|------|-------------|----------------|
| total_applications | BIGINT | All applications | COUNT(*) |
| total_revenue | BIGINT | Successful payments | SUM(payment_amount WHERE status = 'Success') |
| late_completion_rate | DECIMAL(5,2) | % exceeding SLA | COUNT(WHERE is_late) / COUNT(*) * 100 |
| avg_processing_days | DOUBLE | Mean processing time | AVG(processing_days) |

#### Sample Data
```sql
SELECT * FROM atlys.gold.gold_kpi_overview;
```

| total_applications | total_revenue | late_completion_rate | avg_processing_days |
|-------------------|---------------|---------------------|---------------------|
| 250,000 | 21,634,841 | 56.88 | 16.02 |

---

### gold_revenue_metrics
**Purpose**: Revenue metrics by time, geography, and payment attributes  
**Row Count**: ~10,000  
**Grain**: Aggregated by payment_year, payment_month, country, visa_type, payment_method, payment_gateway

#### Schema
| Column | Type | Description | Business Logic |
|--------|------|-------------|----------------|
| payment_year | INT | Transaction year | Dimension |
| payment_month | INT | Transaction month | Dimension |
| payment_quarter | INT | Transaction quarter | Dimension |
| country_name | STRING | Destination country | Dimension |
| visa_type_name | STRING | Visa category | Dimension |
| payment_method | STRING | Payment type | Dimension |
| payment_gateway | STRING | Gateway used | Dimension |
| total_revenue | BIGINT | Sum of successful payments | SUM(payment_amount WHERE status = 'Success') |
| total_transactions | BIGINT | All payment attempts | COUNT(*) |
| successful_payments | BIGINT | Success count | COUNT(WHERE status = 'Success') |
| failed_payments | BIGINT | Failure count | COUNT(WHERE status = 'Failed') |
| avg_transaction_value | DOUBLE | Mean payment amount | AVG(payment_amount WHERE status = 'Success') |

#### Sample Data
```sql
SELECT payment_month, country_name, payment_method, total_revenue, successful_payments
FROM atlys.gold.gold_revenue_metrics
WHERE payment_year = 2024 AND country_name IN ('USA', 'Canada', 'Australia')
LIMIT 10;
```

| payment_year | payment_month | country_name | payment_method | total_revenue | successful_payments | failed_payments |
|--------------|---------------|--------------|----------------|---------------|---------------------|-----------------|
| 2024 | 1 | USA | Credit Card | 425,000 | 4,250 | 120 |
| 2024 | 1 | USA | PayPal | 180,000 | 1,800 | 45 |
| 2024 | 1 | Canada | Credit Card | 320,000 | 3,200 | 95 |
| 2024 | 1 | Australia | Debit Card | 280,000 | 2,800 | 70 |
| 2024 | 2 | USA | Credit Card | 410,000 | 4,100 | 115 |
| 2024 | 2 | Canada | Bank Transfer | 95,000 | 950 | 12 |
| 2024 | 3 | Australia | PayPal | 165,000 | 1,650 | 38 |
| 2024 | 3 | USA | Apple Pay | 125,000 | 1,250 | 25 |
| 2024 | 4 | Canada | Credit Card | 315,000 | 3,150 | 88 |
| 2024 | 4 | Australia | Credit Card | 290,000 | 2,900 | 75 |

---

### gold_abandoned_cart_analysis
**Purpose**: Abandoned application analysis for recovery campaigns  
**Row Count**: ~200  
**Grain**: Aggregated by last_stage_reached, recovery_opportunity, abandonment_reason_category

#### Schema
| Column | Type | Description | Business Logic |
|--------|------|-------------|----------------|
| last_stage_reached | STRING | Final stage before abandonment | From user_events |
| recovery_opportunity | STRING | High/Medium/Low | Scoring algorithm |
| abandonment_reason_category | STRING | Why user quit | Inferred from behavior |
| total_abandoned | BIGINT | Abandoned application count | COUNT(WHERE status IN ('Cancelled', 'Verification Started')) |
| total_potential_revenue | BIGINT | Revenue at risk | SUM(visa_fee_amount) |
| avg_days_since_abandonment | DOUBLE | Days elapsed | AVG(DATEDIFF(NOW(), last_event_date)) |

#### Sample Data
```sql
SELECT * FROM atlys.gold.gold_abandoned_cart_analysis;
```

| last_stage_reached | recovery_opportunity | total_abandoned | total_potential_revenue | avg_days_since_abandonment |
|-------------------|---------------------|-----------------|------------------------|---------------------------|
| Verification Started | Low Recovery Potential | 95,171 | 6,912,423 | 45.3 |
| Cancelled | Low Recovery Potential | 12,619 | 904,924 | 62.1 |

**Note**: Current recovery scoring logic is binary (all marked "Low"). Recommendation is to implement multi-tier scoring based on:
- Stage progression depth (payment initiated = high intent)
- Time since abandonment (recent = higher recoverability)
- Engagement signals (email opens, support contacts)

---

### gold_operational_metrics
**Purpose**: Weekly operational snapshots for health monitoring  
**Row Count**: ~50,000  
**Grain**: Aggregated by application_week, country_name, visa_type_name

#### Schema
| Column | Type | Description | Business Logic |
|--------|------|-------------|----------------|
| application_week | DATE | Week start date (Monday) | DATE_TRUNC('WEEK', application_date) |
| country_name | STRING | Destination country | Dimension |
| visa_type_name | STRING | Visa category | Dimension |
| total_applications | BIGINT | Applications this week | COUNT(*) |
| avg_processing_days | DOUBLE | Mean processing time | AVG(processing_days) |
| avg_late_pct | DOUBLE | % exceeding SLA | AVG(CASE WHEN is_late THEN 100.0 ELSE 0.0 END) |
| completed_count | BIGINT | Completed this week | COUNT(WHERE status IN ('Approved', 'Rejected')) |

#### Sample Data
```sql
SELECT application_week, country_name, visa_type_name, total_applications, avg_processing_days, avg_late_pct
FROM atlys.gold.gold_operational_metrics
WHERE application_week >= '2024-01-01'
ORDER BY application_week DESC
LIMIT 10;
```

| application_week | country_name | visa_type_name | total_applications | avg_processing_days | avg_late_pct |
|------------------|--------------|----------------|--------------------|---------------------|--------------|
| 2024-07-08 | USA | Tourist Visa | 1,250 | 15.8 | 58.4 |
| 2024-07-08 | Canada | Work Permit | 890 | 18.2 | 72.1 |
| 2024-07-08 | Australia | Student Visa | 720 | 14.5 | 48.6 |
| 2024-07-01 | USA | Tourist Visa | 1,180 | 16.1 | 60.2 |
| 2024-07-01 | Canada | Work Permit | 865 | 17.9 | 70.5 |
| 2024-06-24 | Australia | Student Visa | 695 | 15.2 | 52.3 |
| 2024-06-24 | USA | Business Visa | 540 | 12.8 | 35.7 |
| 2024-06-17 | Canada | Tourist Visa | 1,120 | 16.5 | 62.8 |
| 2024-06-17 | Australia | Work Permit | 480 | 19.1 | 78.3 |
| 2024-06-10 | USA | Tourist Visa | 1,205 | 15.6 | 56.9 |

---

## Metric View Definitions

### datasets/revenue_metrics
**Type**: Local Metric View  
**Source**: atlys.gold.gold_revenue_metrics

#### Dimensions
- `payment_year` (INT)
- `payment_month` (INT)
- `payment_month_date` (DATE) — Derived: `MAKE_DATE(payment_year, payment_month, 1)`
- `payment_quarter` (INT)
- `country_name` (STRING)
- `visa_type_name` (STRING)
- `payment_method` (STRING)
- `payment_gateway` (STRING)

#### Measures
- `total_revenue`: `SUM(total_revenue)` — Total Revenue
- `total_transactions`: `SUM(total_transactions)` — Total Transactions
- `successful_payments`: `SUM(successful_payments)` — Successful Payments
- `failed_payments`: `SUM(failed_payments)` — Failed Payments
- `avg_transaction_value`: `AVG(avg_transaction_value)` — Avg Transaction Value
- `payment_success_rate`: `SUM(successful_payments) * 100.0 / NULLIF(SUM(successful_payments) + SUM(failed_payments), 0)` — Payment Success Rate %

#### Example Query
```sql
SELECT 
  payment_month_date,
  country_name,
  MEASURE(total_revenue) AS total_revenue,
  MEASURE(payment_success_rate) AS success_rate_pct
FROM datasets.revenue_metrics
WHERE payment_year = 2024
ORDER BY payment_month_date, total_revenue DESC;
```

---

## Data Quality Rules

### Bronze Layer
- **Immutability**: No updates or deletes; append-only
- **Audit**: All tables have `ingestion_timestamp`
- **Retention**: Keep raw data indefinitely for reprocessing

### Silver Layer
- **Deduplication**: Window function on PK ordered by latest event
- **Referential Integrity**: All FKs validated against dimension tables
- **Range Validation**: 
  - Dates between 2020-01-01 and current date
  - Amounts > 0
  - Processing days ≥ 0
- **Enum Validation**: Status fields validated against approved values

### Gold Layer
- **Completeness**: No NULL measures (use 0 or COALESCE)
- **Grain Enforcement**: Primary key on dimension combinations
- **Aggregation Logic**: Documented in schema comments
- **Refresh Cadence**: Daily at 2 AM UTC

---

## Change Log

| Date | Table | Change | Author |
|------|-------|--------|--------|
| 2024-07-14 | gold_revenue_metrics | Added payment_success_rate measure | Rishi Gupta |
| 2024-07-14 | datasets/revenue_metrics | Added payment_month_date dimension | Rishi Gupta |
| 2024-07-13 | silver_applications | Added status_category derived field | Rishi Gupta |
| 2024-07-12 | gold_kpi_overview | Initial creation | Rishi Gupta |

---
