-- Databricks notebook source
select * from gameanalytics.raw_users

-- COMMAND ----------

select * from gameanalytics.raw_logins

-- COMMAND ----------

select * from gameanalytics.raw_game_sessions

-- COMMAND ----------

select * from gameanalytics.raw_purchases

-- COMMAND ----------

select * from gameanalytics.raw_leaderboard_snapshots

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # user categories
-- MAGIC # what times are most active
-- MAGIC # what country has most whales
-- MAGIC # what is the constitution of the user base(age/gender/ethnicity)
-- MAGIC # what devices are most actively used
-- MAGIC # total earnings from the purchases (future: ad revenue?)
-- MAGIC

-- COMMAND ----------

select * from gameanalytics.silver_user_sessions limit 100

-- COMMAND ----------

