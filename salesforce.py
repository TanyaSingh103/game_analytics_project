# Databricks notebook source
# MAGIC %pip install simple-salesforce

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gameanalytics.raw_users limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gameanalytics.raw_users__c

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into gameanalytics.raw_users
# MAGIC (
# MAGIC   select user_id__c as user_id,
# MAGIC          username__c as username,
# MAGIC          country__c as country,
# MAGIC          signup_date__c as signup_date,
# MAGIC          age__c as age,
# MAGIC          gender__c as gender
# MAGIC   from gameanalytics.raw_users__c
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into gameanalytics.raw_logins
# MAGIC (
# MAGIC   select user_id__c as user_id,
# MAGIC          timestamp__c as timestamp,
# MAGIC          device_type__c as device_type,
# MAGIC          location__c as location,
# MAGIC          event_date__c as event_date
# MAGIC   from gameanalytics.raw_logins__c
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into gameanalytics.raw_game_sessions
# MAGIC (
# MAGIC   select session_id__c as session_id,
# MAGIC          user_id__c as user_id,
# MAGIC          game_type__c as game_type,
# MAGIC          start_time__c as start_time,
# MAGIC          end_time__c as end_time,
# MAGIC          score__c as score,
# MAGIC          result__c as result,
# MAGIC          event_date__c as event_date
# MAGIC   from gameanalytics.raw_game_sessions__c
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into gameanalytics.raw_purchases
# MAGIC (
# MAGIC   select purchase_id__c as purchase_id,
# MAGIC          user_id__c as user_id,
# MAGIC          item__c as item,
# MAGIC          amount__c as amount,
# MAGIC          timestamp__c as timestamp,
# MAGIC          event_date__c as event_date
# MAGIC   from gameanalytics.raw_purchases__c
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into gameanalytics.raw_leaderboard_snapshots
# MAGIC (
# MAGIC   select user_id__c as user_id,
# MAGIC          game_type__c as game_type,
# MAGIC          rank__c as rank,
# MAGIC          event_date__c as event_date
# MAGIC   from gameanalytics.raw_leaderboard_snapshots__c
# MAGIC )

# COMMAND ----------

