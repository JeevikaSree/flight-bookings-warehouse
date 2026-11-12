# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — incremental load of one daily drop
# MAGIC Loads only the files for `load_date`. Re-running the same date replaces that
# MAGIC date's partition (`replaceWhere`), so the load is **idempotent**: no duplicates
# MAGIC if a job is retried.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "flights")
dbutils.widgets.text("load_date", "2026-01-01")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
load_date = dbutils.widgets.get("load_date")
raw = f"/Volumes/{catalog}/{schema}/raw"
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

def load(entity: str) -> int:
    path = f"{raw}/{entity}/{entity}_{load_date}.csv"
    try:
        dbutils.fs.ls(path)
    except Exception:
        print(f"{entity}: no file for {load_date}, skipped")
        return 0
    df = (spark.read.option("header", True).csv(path)
          .withColumn("_load_date", F.lit(load_date).cast("date"))
          .withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_source_file", F.col("_metadata.file_path")))
    (df.write.format("delta").mode("overwrite")
       .option("replaceWhere", f"_load_date = '{load_date}'")
       .option("mergeSchema", True)
       .saveAsTable(f"bronze_{entity}"))
    n = df.count()
    print(f"bronze_{entity}: {n:,} rows for {load_date}")
    return n

for entity in ["airports", "flights", "customers", "bookings"]:
    load(entity)
