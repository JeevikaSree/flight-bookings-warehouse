# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Validate the warehouse
# MAGIC Fails the job if SCD2 history or fact keys are broken.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "flights")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

q = lambda sql: spark.sql(sql).first()[0]

checks = {
    "One current row per customer":
        q("SELECT COUNT(*) FROM (SELECT customer_id FROM dim_customer WHERE is_current GROUP BY customer_id HAVING COUNT(*) <> 1)") == 0,
    "No overlapping SCD2 date ranges":
        q("""SELECT COUNT(*) FROM dim_customer a JOIN dim_customer b
             ON a.customer_id = b.customer_id AND a.customer_sk < b.customer_sk
             AND a.effective_from <= b.effective_to AND b.effective_from <= a.effective_to""") == 0,
    "booking_id unique in fact":
        q("SELECT COUNT(*) - COUNT(DISTINCT booking_id) FROM fact_bookings") == 0,
    "Every booking has a customer version":
        q("SELECT COUNT(*) FROM fact_bookings WHERE customer_sk IS NULL") == 0,
    "Every booking has a known flight":
        q("SELECT COUNT(*) FROM fact_bookings f LEFT ANTI JOIN dim_flight d ON f.flight_id = d.flight_id") == 0,
}
for name, ok in checks.items():
    print(("PASS  " if ok else "FAIL  ") + name)
assert all(checks.values()), "Warehouse validation failed"

# COMMAND ----------

# MAGIC %md ## See SCD Type 2 history for customers who changed

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, customer_sk, city, loyalty_tier, effective_from, effective_to, is_current
# MAGIC FROM dim_customer
# MAGIC WHERE customer_id IN (SELECT customer_id FROM dim_customer GROUP BY customer_id HAVING COUNT(*) > 1)
# MAGIC ORDER BY customer_id, effective_from
# MAGIC LIMIT 20
