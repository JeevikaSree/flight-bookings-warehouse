# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Creates the schema, the raw volume, and the warehouse tables.
# MAGIC Then upload the `data/raw/` folders (airports, flights, customers, bookings)
# MAGIC into the volume, keeping the sub-folder names.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "flights")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer dimension: SCD Type 2 (full history of city / loyalty tier changes)
# MAGIC CREATE TABLE IF NOT EXISTS dim_customer (
# MAGIC   customer_sk     BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   customer_id     STRING,
# MAGIC   full_name       STRING,
# MAGIC   city            STRING,
# MAGIC   loyalty_tier    STRING,
# MAGIC   row_hash        STRING,
# MAGIC   effective_from  DATE,
# MAGIC   effective_to    DATE,
# MAGIC   is_current      BOOLEAN
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Airport and flight dimensions: SCD Type 1 (overwrite in place)
# MAGIC CREATE TABLE IF NOT EXISTS dim_airport (airport_code STRING, city STRING, country STRING) USING DELTA;
# MAGIC CREATE TABLE IF NOT EXISTS dim_flight  (flight_id STRING, airline STRING, origin STRING, destination STRING, duration_min INT) USING DELTA;
# MAGIC
# MAGIC -- Fact: one row per booking, upserted as statuses change
# MAGIC CREATE TABLE IF NOT EXISTS fact_bookings (
# MAGIC   booking_id    STRING,
# MAGIC   customer_sk   BIGINT,
# MAGIC   customer_id   STRING,
# MAGIC   flight_id     STRING,
# MAGIC   booking_date  DATE,
# MAGIC   travel_date   DATE,
# MAGIC   cabin         STRING,
# MAGIC   fare          DECIMAL(10,2),
# MAGIC   status        STRING,
# MAGIC   last_updated  DATE
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Watermark: which daily drops have been loaded
# MAGIC CREATE TABLE IF NOT EXISTS etl_load_log (load_date DATE, loaded_at TIMESTAMP) USING DELTA;
