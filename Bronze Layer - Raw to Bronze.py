# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Bronze Layer Overview
# MAGIC %md
# MAGIC # Bronze Layer - Raw to Bronze
# MAGIC
# MAGIC ## Medallion Architecture - Bronze Layer
# MAGIC
# MAGIC This notebook implements the **Bronze layer** of the Medallion architecture for ATLYS visa platform data.
# MAGIC
# MAGIC ### Purpose
# MAGIC - Copy data from `Atlys.raw` schema to `Atlys.bronze` schema
# MAGIC - Bronze layer serves as an immutable copy of raw data
# MAGIC - Minimal to no transformations
# MAGIC - Preserves data lineage and auditability
# MAGIC
# MAGIC ### Data Flow
# MAGIC ```
# MAGIC Atlys.raw.* → Atlys.bronze.*
# MAGIC ```
# MAGIC
# MAGIC ### Tables to Process
# MAGIC 1. raw_countries → bronze_countries
# MAGIC 2. raw_visa_types → bronze_visa_types
# MAGIC 3. raw_airports → bronze_airports
# MAGIC 4. raw_users → bronze_users
# MAGIC 5. raw_passports → bronze_passports
# MAGIC 6. raw_applications → bronze_applications
# MAGIC 7. raw_payments → bronze_payments
# MAGIC 8. raw_documents → bronze_documents
# MAGIC 9. raw_application_events → bronze_application_events
# MAGIC 10. raw_reviews → bronze_reviews
# MAGIC 11. raw_support_tickets → bronze_support_tickets

# COMMAND ----------

# DBTITLE 1,Setup and Configuration
# Configuration for Bronze layer
from pyspark.sql import SparkSession
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Configuration
SOURCE_CATALOG = "Atlys"
SOURCE_SCHEMA = "raw"
TARGET_CATALOG = "Atlys"
TARGET_SCHEMA = "bronze"

# Table mappings (raw -> bronze)
TABLES = [
    "countries",
    "visa_types",
    "airports",
    "users",
    "passports",
    "applications",
    "payments",
    "documents",
    "application_events",
    "reviews",
    "support_tickets"
]

print(f"Source: {SOURCE_CATALOG}.{SOURCE_SCHEMA}")
print(f"Target: {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"Tables to process: {len(TABLES)}")

# COMMAND ----------

# DBTITLE 1,Create Bronze Schema
# Create bronze schema if it doesn't exist
print("Creating Bronze schema...")

# Create catalog if needed (skip for 'Atlys' as it already exists)
if TARGET_CATALOG not in ["main", "hive_metastore"]:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {TARGET_CATALOG}")
    print(f"✓ Catalog '{TARGET_CATALOG}' ready")

# Create bronze schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"✓ Schema '{TARGET_CATALOG}.{TARGET_SCHEMA}' created")

# Verify schema exists
schemas = spark.sql(f"SHOW SCHEMAS IN {TARGET_CATALOG}").collect()
schema_names = [row.databaseName for row in schemas]
print(f"\nAvailable schemas in {TARGET_CATALOG}: {schema_names}")

# COMMAND ----------

# DBTITLE 1,Bronze Layer Copy Function
def copy_raw_to_bronze(table_name: str, mode: str = "overwrite"):
    """
    Copy data from raw to bronze layer with minimal transformations.
    
    Args:
        table_name: Name of the table (without schema prefix)
        mode: Write mode - 'overwrite' or 'append'
    """
    source_table = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.raw_{table_name}"
    target_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.bronze_{table_name}"
    
    try:
        print(f"\n📦 Processing: {table_name}")
        print(f"   Source: {source_table}")
        print(f"   Target: {target_table}")
        
        # Read from raw layer
        df = spark.table(source_table)
        row_count = df.count()
        print(f"   Rows: {row_count:,}")
        
        # Add bronze layer metadata columns
        from pyspark.sql.functions import current_timestamp, lit
        
        df_bronze = df \
            .withColumn("bronze_ingestion_timestamp", current_timestamp()) \
            .withColumn("bronze_source_table", lit(source_table))
        
        # Write to bronze layer
        df_bronze.write \
            .format("delta") \
            .mode(mode) \
            .option("overwriteSchema", "true") \
            .saveAsTable(target_table)
        
        print(f"   ✓ Success: Copied {row_count:,} rows to {target_table}")
        return True, row_count
        
    except Exception as e:
        print(f"   ✗ Error: {str(e)}")
        return False, 0

print("✓ Bronze copy function defined")

# COMMAND ----------

# DBTITLE 1,Execute Bronze Layer Copy
# Execute bronze layer copy for all tables
print("=" * 80)
print("BRONZE LAYER INGESTION - Raw to Bronze")
print("=" * 80)

results = []
total_rows = 0

for table in TABLES:
    success, row_count = copy_raw_to_bronze(table, mode="overwrite")
    results.append({"table": table, "success": success, "rows": row_count})
    total_rows += row_count

print("\n" + "=" * 80)
print("BRONZE LAYER INGESTION COMPLETE")
print("=" * 80)

# Summary
successful = sum(1 for r in results if r["success"])
failed = len(results) - successful

print(f"\n📊 Summary:")
print(f"   Total tables: {len(results)}")
print(f"   Successful: {successful}")
print(f"   Failed: {failed}")
print(f"   Total rows copied: {total_rows:,}")

# Show results table
import pandas as pd
df_results = pd.DataFrame(results)
display(df_results)

# COMMAND ----------

# DBTITLE 1,Verify Bronze Tables
# MAGIC %sql
# MAGIC -- Verify all bronze tables exist
# MAGIC SHOW TABLES IN Atlys.bronze

# COMMAND ----------

# DBTITLE 1,Sample Data Verification
# MAGIC %sql
# MAGIC -- Sample data from bronze users table
# MAGIC SELECT 
# MAGIC     user_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     email,
# MAGIC     country,
# MAGIC     signup_date,
# MAGIC     bronze_ingestion_timestamp,
# MAGIC     bronze_source_table
# MAGIC FROM Atlys.bronze.bronze_users
# MAGIC LIMIT 10
