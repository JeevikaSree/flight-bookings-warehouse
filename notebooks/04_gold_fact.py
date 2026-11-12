# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold — fact_bookings (incremental MERGE)
# MAGIC * New bookings are inserted; bookings whose status changed (CANCELLED, CHECKED_IN)
# MAGIC   are updated in place — an **upsert** with `MERGE INTO`.
# MAGIC * `customer_sk` points to the customer version that was valid on the booking date,
# MAGIC   so reports show the customer's city/tier *as it was* when they booked.
# MAGIC * Finally the day is recorded in `etl_load_log` (the watermark).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "flights")
dbutils.widgets.text("load_date", "2026-01-01")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
load_date = dbutils.widgets.get("load_date")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

spark.sql(f"""
    MERGE INTO fact_bookings t
    USING (
        SELECT b.booking_id, d.customer_sk, b.customer_id, b.flight_id, b.booking_date,
               b.travel_date, b.cabin, b.fare, b.status, b.last_updated
        FROM silver_bookings b
        LEFT JOIN dim_customer d
          ON b.customer_id = d.customer_id
         AND b.booking_date BETWEEN d.effective_from AND d.effective_to
        WHERE b._load_date = '{load_date}'
    ) s
    ON t.booking_id = s.booking_id
    WHEN MATCHED AND s.last_updated >= t.last_updated THEN UPDATE SET
        t.status = s.status,
        t.travel_date = s.travel_date,
        t.fare = s.fare,
        t.last_updated = s.last_updated
    WHEN NOT MATCHED THEN INSERT *
""")

spark.sql(f"""
    MERGE INTO etl_load_log t
    USING (SELECT DATE'{load_date}' AS load_date, current_timestamp() AS loaded_at) s
    ON t.load_date = s.load_date
    WHEN MATCHED THEN UPDATE SET t.loaded_at = s.loaded_at
    WHEN NOT MATCHED THEN INSERT *
""")

display(spark.sql("SELECT status, COUNT(*) AS bookings FROM fact_bookings GROUP BY status ORDER BY bookings DESC"))
