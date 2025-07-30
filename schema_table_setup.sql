-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS gameanalytics;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gameanalytics.raw_users (
  user_id STRING,
  username STRING,
  country STRING,
  signup_date DATE,
  age INT,
  gender STRING
)
USING DELTA;


-- COMMAND ----------

ALTER TABLE gameanalytics.raw_users
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gameanalytics.raw_logins (
  user_id STRING,
  timestamp TIMESTAMP,
  device_type STRING,
  location STRING,
  event_date DATE       
)
USING DELTA
PARTITIONED BY (event_date);  

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gameanalytics.raw_game_sessions (
  session_id STRING,
  user_id STRING,
  game_type STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  score INT,
  result STRING
)
USING DELTA
PARTITIONED BY (game_type);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gameanalytics.raw_purchases (
  purchase_id STRING,
  user_id STRING,
  item STRING,
  amount DOUBLE,
  timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (item);


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gameanalytics.raw_leaderboard_snapshots (
  user_id STRING,
  game_type STRING,
  rank INT,
  date DATE
)
USING DELTA
PARTITIONED BY (game_type);


-- COMMAND ----------

ALTER TABLE gameanalytics.raw_game_sessions ADD COLUMNS (event_date DATE);
ALTER TABLE gameanalytics.raw_purchases ADD COLUMNS (event_date DATE);
ALTER TABLE gameanalytics.raw_leaderboard_snapshots ADD COLUMNS (event_date DATE);


-- COMMAND ----------

ALTER TABLE gameanalytics.raw_leaderboard_snapshots 
SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');


-- COMMAND ----------

ALTER TABLE gameanalytics.raw_leaderboard_snapshots DROP COLUMN date;

-- COMMAND ----------

DESCRIBE TABLE gameanalytics.raw_leaderboard_snapshots;


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gameanalytics.silver_user_sessions (
  session_id STRING,
  user_id STRING,
  game_type STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  session_duration_minutes DOUBLE,
  score INT,
  result STRING,
  age INT,
  gender STRING,
  country STRING,
  login_device STRING,
  login_location STRING,
  event_date DATE
)
USING DELTA


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gameanalytics.silver_purchases_enriched (
  purchase_id STRING,
  user_id STRING,
  item STRING,
  amount DOUBLE,
  timestamp TIMESTAMP,
  age INT,
  gender STRING,
  country STRING,
  signup_date DATE,
  event_date DATE
)
USING DELTA


-- COMMAND ----------


CREATE TABLE IF NOT EXISTS gameanalytics.silver_leaderboard_enriched (
  user_id STRING,
  game_type STRING,
  rank INT,
  score INT,
  age INT,
  gender STRING,
  country STRING,
  event_date DATE
)
USING DELTA


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gameanalytics.gold_daily_summary (
  event_date DATE,
  total_sessions INT,
  avg_session_duration DOUBLE,
  unique_active_users INT,
  total_revenue DOUBLE,
  avg_purchase_amount DOUBLE,
  top_device_type STRING,
  peak_hour INT
)
USING DELTA


-- COMMAND ----------


CREATE TABLE IF NOT EXISTS gameanalytics.gold_user_demographics (
  user_id STRING,
  gender STRING,
  age_bracket STRING,
  country STRING,
  total_sessions INT,
  avg_session_duration DOUBLE,
  total_spent DOUBLE,
  num_purchases INT
)
USING DELTA

-- COMMAND ----------

ALTER TABLE gameanalytics.gold_user_demographics ADD COLUMNS (user_segment STRING);


-- COMMAND ----------

