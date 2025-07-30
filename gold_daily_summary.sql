-- Databricks notebook source
-- ALTER TABLE gameanalytics.gold_daily_summary SET TBLPROPERTIES (
--   delta.enableDeletionVectors = true,
--   delta.enableRowTracking = true,
--   delta.enableChangeDataFeed = true);

-- COMMAND ----------

-- describe history gameanalytics.gold_daily_summary

-- COMMAND ----------

-- select * from table_changes('gameanalytics.gold_daily_summary', 52)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("run_date", "")
-- MAGIC run_date = dbutils.widgets.get("run_date")
-- MAGIC
-- MAGIC from datetime import datetime
-- MAGIC run_date = datetime.strptime(run_date, "%Y-%m-%d").date()

-- COMMAND ----------


CREATE OR REPLACE TEMP VIEW gold_daily_summary_view AS
WITH sessions AS (
  SELECT
    COUNT(*) AS total_sessions,
    ROUND(AVG(session_duration_minutes), 2) AS avg_session_duration,
    COUNT(DISTINCT user_id) AS unique_active_users
  FROM gameanalytics.silver_user_sessions
  WHERE event_date = DATE('$run_date')
),

peak_hour AS (
  SELECT hour_val AS peak_hour
  FROM (
    SELECT HOUR(start_time) AS hour_val, COUNT(*) AS cnt
    FROM gameanalytics.silver_user_sessions
    WHERE event_date = DATE('$run_date')
    GROUP BY HOUR(start_time)
    ORDER BY cnt DESC
    LIMIT 1
  )
),

top_device AS (
  SELECT login_device AS top_device_type
  FROM (
    SELECT login_device, COUNT(*) AS cnt
    FROM gameanalytics.silver_user_sessions
    WHERE event_date = DATE('$run_date')
    GROUP BY login_device
    ORDER BY cnt DESC
    LIMIT 1
  )
),

revenue AS (
  SELECT
    ROUND(SUM(amount), 2) AS total_revenue,
    ROUND(AVG(amount), 2) AS avg_purchase_amount
  FROM gameanalytics.silver_purchases_enriched
  WHERE event_date = DATE('$run_date')
)

SELECT
  DATE('$run_date') AS event_date,
  s.total_sessions,
  s.avg_session_duration,
  s.unique_active_users,
  r.total_revenue,
  r.avg_purchase_amount,
  d.top_device_type,
  p.peak_hour

FROM sessions s, revenue r, top_device d, peak_hour p

-- COMMAND ----------

-- INSERT INTO gameanalytics.gold_daily_summary
-- SELECT * FROM gold_daily_summary_view


-- COMMAND ----------

MERGE INTO gameanalytics.gold_daily_summary AS target
USING gold_daily_summary_view AS source
ON target.event_date = source.event_date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;


-- COMMAND ----------

