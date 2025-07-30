# Databricks notebook source
dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")

from datetime import datetime
run_date = datetime.strptime(run_date, "%Y-%m-%d").date()

# COMMAND ----------


from faker import Faker
import uuid
import random
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession

fake = Faker()
spark = SparkSession.builder.getOrCreate()


# COMMAND ----------

def generate_users(n=1000):
    users = []
    for _ in range(n):
        users.append({
            "user_id": str(uuid.uuid4()),
            "username": fake.user_name(),
            "country": fake.country(),
            "signup_date": fake.date_between(start_date='-20d', end_date='-1d'),
            "age": random.randint(13, 45),
            "gender": random.choice(["Male", "Female", "Other"])
        })

    df = pd.DataFrame(users)
    spark_df = spark.createDataFrame(df)

    from pyspark.sql.functions import col, to_date
    spark_df = spark_df.withColumn("age", col("age").cast("int"))
    spark_df = spark_df.withColumn("signup_date", to_date(col("signup_date")))

    spark_df.write.mode("append").format("delta").saveAsTable("gameanalytics.raw_users")


# COMMAND ----------

generate_users(100000)

# COMMAND ----------

df = spark.sql("SELECT * FROM gameanalytics.raw_users")

df.toPandas().to_csv("/Workspace/Users/tanya.singh@eucloid.com/Notebooks/game_analytics_project/csvs/raw_users_2.csv", index=False)



# COMMAND ----------

# Use the connection name instead of individual credentials
df = spark.read.format("SALESFORCE").option("connectionName", "tanya_game_analytics").option("sfObject", "Account").load()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gameanalytics.raw_users limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view users_temp as select * from gameanalytics.raw_users cluster by signup_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from users_temp limit 50

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from users_temp