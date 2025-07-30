-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("run_date", "")
-- MAGIC run_date = dbutils.widgets.get("run_date")
-- MAGIC
-- MAGIC from datetime import datetime
-- MAGIC run_date = datetime.strptime(run_date, "%Y-%m-%d").date()

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW cleaned_leaderboard AS
SELECT
  l.user_id,
  l.game_type,
  l.rank,
  s.score,
  u.age,
  u.gender,
  u.country,
  l.event_date

FROM gameanalytics.raw_leaderboard_snapshots l

LEFT JOIN gameanalytics.raw_users u
  ON l.user_id = u.user_id

LEFT JOIN gameanalytics.raw_game_sessions s
  ON l.user_id = s.user_id
  AND l.game_type = s.game_type
  AND l.event_date = s.event_date

WHERE l.event_date = DATE('$run_date')
  AND l.user_id IS NOT NULL
  AND l.rank > 0


-- COMMAND ----------

INSERT INTO gameanalytics.silver_leaderboard_enriched
SELECT * FROM cleaned_leaderboard
