-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("run_date", "")
-- MAGIC run_date = dbutils.widgets.get("run_date")
-- MAGIC
-- MAGIC from datetime import datetime
-- MAGIC run_date = datetime.strptime(run_date, "%Y-%m-%d").date()

-- COMMAND ----------

-- CREATE OR REPLACE TEMP VIEW cleaned_user_sessions AS
-- SELECT
--   s.session_id,
--   s.user_id,
--   s.game_type,
--   s.start_time,
--   s.end_time,
--   ROUND((UNIX_TIMESTAMP(s.end_time) - UNIX_TIMESTAMP(s.start_time)) / 60.0, 1) AS session_duration_minutes,
--   s.score,
--   INITCAP(LOWER(s.result)) AS result,
--   u.age,
--   u.gender,
--   u.country,
--   INITCAP(LOWER(l.device_type)) AS login_device,
--   l.location AS login_location,
--   s.event_date

-- FROM gameanalytics.raw_game_sessions s
-- LEFT JOIN gameanalytics.raw_users u
--   ON s.user_id = u.user_id
-- LEFT JOIN (
--     SELECT user_id, device_type, location, event_date
--     FROM gameanalytics.raw_logins
--     WHERE event_date = DATE('$run_date')
-- ) l
--   ON s.user_id = l.user_id AND s.event_date = l.event_date

-- WHERE s.event_date = DATE('$run_date')


-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col, lower, initcap, round, unix_timestamp, broadcast
-- MAGIC
-- MAGIC # Load and filter raw_game_sessions
-- MAGIC sessions = spark.table("gameanalytics.raw_game_sessions").filter(col("event_date") == run_date)
-- MAGIC
-- MAGIC # Load and broadcast raw_users
-- MAGIC users = broadcast(spark.table("gameanalytics.raw_users"))
-- MAGIC
-- MAGIC # Load and filter raw_logins for run_date
-- MAGIC logins = broadcast(spark.sql(f"""
-- MAGIC   SELECT user_id, device_type, location, event_date
-- MAGIC   FROM gameanalytics.raw_logins
-- MAGIC   WHERE event_date = DATE('{run_date}')
-- MAGIC """))
-- MAGIC
-- MAGIC # Join logic
-- MAGIC joined = sessions.join(users, on="user_id", how="left") \
-- MAGIC     .join(logins, on=["user_id", "event_date"], how="left") \
-- MAGIC     .withColumn("session_duration_minutes", round(
-- MAGIC         (unix_timestamp("end_time") - unix_timestamp("start_time")) / 60.0, 1)) \
-- MAGIC     .withColumn("result", initcap(lower("result"))) \
-- MAGIC     .withColumn("login_device", initcap(lower("device_type"))) \
-- MAGIC     .select(
-- MAGIC         "session_id", "user_id", "game_type", "start_time", "end_time",
-- MAGIC         "session_duration_minutes", "score", "result", "age", "gender", "country",
-- MAGIC         "login_device", col("location").alias("login_location"), "event_date"
-- MAGIC     )
-- MAGIC
-- MAGIC # Register as temp view
-- MAGIC joined.createOrReplaceTempView("cleaned_user_sessions")
-- MAGIC

-- COMMAND ----------

-- select * from cleaned_user_sessions

-- COMMAND ----------

INSERT INTO gameanalytics.silver_user_sessions
SELECT * FROM cleaned_user_sessions


-- COMMAND ----------

OPTIMIZE gameanalytics.silver_user_sessions
ZORDER BY (user_id, event_date);


-- COMMAND ----------



-- COMMAND ----------

