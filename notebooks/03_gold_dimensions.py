# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — dimensions
# MAGIC * `dim_airport`, `dim_flight`: **SCD Type 1** — MERGE overwrites changed attributes.
# MAGIC * `dim_customer`: **SCD Type 2** — when a customer's city or loyalty tier changes,
# MAGIC   the current row is closed (`effective_to`, `is_current = false`) and a new
# MAGIC   current row is inserted. History is never overwritten.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "flights")
dbutils.widgets.text("load_date", "2026-01-01")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
load_date = dbutils.widgets.get("load_date")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

def has_rows(table):
    return spark.catalog.tableExists(table) and \
        spark.sql(f"SELECT 1 FROM {table} WHERE _load_date = '{load_date}' LIMIT 1").count() > 0

# COMMAND ----------

# MAGIC %md ## SCD Type 1 — airports and flights

# COMMAND ----------

if has_rows("silver_airports"):
    spark.sql(f"""
        MERGE INTO dim_airport t
        USING (SELECT airport_code, city, country FROM silver_airports WHERE _load_date = '{load_date}') s
        ON t.airport_code = s.airport_code
        WHEN MATCHED THEN UPDATE SET t.city = s.city, t.country = s.country
        WHEN NOT MATCHED THEN INSERT *
    """)

if has_rows("silver_flights"):
    spark.sql(f"""
        MERGE INTO dim_flight t
        USING (SELECT flight_id, airline, origin, destination, duration_min
               FROM silver_flights WHERE _load_date = '{load_date}') s
        ON t.flight_id = s.flight_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 2 — customers
# MAGIC The source is staged twice:
# MAGIC 1. every incoming row with `merge_key = customer_id` → matches the current row; if the hash
# MAGIC    changed, that current row is **closed**;
# MAGIC 2. changed rows again with `merge_key = NULL` → never match, so they are **inserted** as the new current version.
# MAGIC
# MAGIC Brand-new customers match nothing in step 1 and are inserted. Unchanged customers match but
# MAGIC are left alone, so re-running a day changes nothing.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW stg_customer AS
    SELECT customer_id, full_name, city, loyalty_tier,
           sha2(concat_ws('|', full_name, city, loyalty_tier), 256) AS row_hash,
           DATE'{load_date}' AS effective_from
    FROM silver_customers
    WHERE _load_date = '{load_date}'
""")

spark.sql("""
    MERGE INTO dim_customer t
    USING (
        SELECT s.customer_id AS merge_key, s.* FROM stg_customer s
        UNION ALL
        SELECT NULL AS merge_key, s.*
        FROM stg_customer s
        JOIN dim_customer d
          ON s.customer_id = d.customer_id AND d.is_current AND d.row_hash <> s.row_hash
    ) u
    ON t.customer_id = u.merge_key AND t.is_current
    WHEN MATCHED AND t.row_hash <> u.row_hash THEN UPDATE SET
        t.is_current = false,
        t.effective_to = date_sub(u.effective_from, 1)
    WHEN NOT MATCHED THEN INSERT
        (customer_id, full_name, city, loyalty_tier, row_hash, effective_from, effective_to, is_current)
        VALUES (u.customer_id, u.full_name, u.city, u.loyalty_tier, u.row_hash,
                u.effective_from, DATE'9999-12-31', true)
""")

display(spark.sql("""
    SELECT COUNT(*) AS total_rows,
           SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows,
           SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END) AS history_rows
    FROM dim_customer
"""))
