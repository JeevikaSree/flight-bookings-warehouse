# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — clean and de-duplicate the day's drop
# MAGIC * Types cast, text trimmed and standardised.
# MAGIC * Duplicates inside the drop removed (latest `last_updated` wins).
# MAGIC * Output tables are partitioned by `_load_date` so Gold reads only the new day.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "flights")
dbutils.widgets.text("load_date", "2026-01-01")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
load_date = dbutils.widgets.get("load_date")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

def save_day(df, table):
    (df.write.format("delta").mode("overwrite")
       .option("replaceWhere", f"_load_date = '{load_date}'")
       .saveAsTable(table))
    print(f"{table}: {df.count():,} rows for {load_date}")

def day(table):
    return spark.table(table).filter(F.col("_load_date") == load_date)

def latest(df, key, order_col):
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

# COMMAND ----------

customers = (day("bronze_customers")
    .select(F.trim("customer_id").alias("customer_id"),
            F.initcap(F.trim("full_name")).alias("full_name"),
            F.initcap(F.trim("city")).alias("city"),
            F.upper(F.trim("loyalty_tier")).alias("loyalty_tier"),
            "_load_date", "_ingest_ts")
    .filter(F.col("customer_id").isNotNull()))
save_day(latest(customers, "customer_id", "_ingest_ts"), "silver_customers")

bookings = (day("bronze_bookings")
    .select(F.trim("booking_id").alias("booking_id"),
            F.trim("customer_id").alias("customer_id"),
            F.trim("flight_id").alias("flight_id"),
            F.expr("to_date(try_to_timestamp(booking_date, 'yyyy-MM-dd'))").alias("booking_date"),
            F.expr("to_date(try_to_timestamp(travel_date, 'yyyy-MM-dd'))").alias("travel_date"),
            F.upper(F.trim("cabin")).alias("cabin"),
            F.expr("try_cast(fare AS DECIMAL(10,2))").alias("fare"),
            F.upper(F.trim("status")).alias("status"),
            F.expr("to_date(try_to_timestamp(last_updated, 'yyyy-MM-dd'))").alias("last_updated"),
            "_load_date", "_ingest_ts")
    .filter("booking_id IS NOT NULL AND booking_date IS NOT NULL AND fare > 0"))
save_day(latest(bookings, "booking_id", "last_updated"), "silver_bookings")

# Reference data arrives only on some days
if day("bronze_flights").limit(1).count() if spark.catalog.tableExists("bronze_flights") else False:
    save_day(day("bronze_flights").select("flight_id", "airline", "origin", "destination",
             F.col("duration_min").cast("int").alias("duration_min"), "_load_date"), "silver_flights")
if day("bronze_airports").limit(1).count() if spark.catalog.tableExists("bronze_airports") else False:
    save_day(day("bronze_airports").select("airport_code", "city", "country", "_load_date"), "silver_airports")
