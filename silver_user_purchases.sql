-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("run_date", "")
-- MAGIC run_date = dbutils.widgets.get("run_date")
-- MAGIC
-- MAGIC from datetime import datetime
-- MAGIC run_date = datetime.strptime(run_date, "%Y-%m-%d").date()

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW cleaned_purchases AS
SELECT
  p.purchase_id,
  p.user_id,
  INITCAP(p.item) AS item,
  ROUND(p.amount, 2) AS amount,
  p.timestamp,
  u.age,
  u.gender,
  u.country,
  u.signup_date,
  p.event_date

FROM gameanalytics.raw_purchases p
LEFT JOIN gameanalytics.raw_users u
  ON p.user_id = u.user_id

WHERE p.event_date = DATE('$run_date')
  AND p.user_id IS NOT NULL
  AND p.amount > 0

-- COMMAND ----------

INSERT INTO gameanalytics.silver_purchases_enriched
SELECT * FROM cleaned_purchases
