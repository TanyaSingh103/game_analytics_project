# Databricks notebook source
dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")

from datetime import datetime
run_date = datetime.strptime(run_date, "%Y-%m-%d").date()


# COMMAND ----------

!pip install faker

# COMMAND ----------

# Imports
from faker import Faker
import uuid
import random
from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql import SparkSession

fake = Faker()
spark = SparkSession.builder.getOrCreate()


# COMMAND ----------

def generate_users(n=10):
    users = []
    for _ in range(n):
        users.append({
            "user_id": str(uuid.uuid4()),
            "username": fake.user_name(),
            "country": fake.country(),
            "signup_date": fake.date_between(start_date='-5d', end_date='-1d'),
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

def get_users():
    return spark.table("gameanalytics.raw_users").collect()


# COMMAND ----------

def generate_logins(users, event_date):
    rows = []
    for user in users:
        if random.random() < 0.80:
            rows.append({
                "user_id": user.user_id,
                "timestamp": datetime.combine(event_date, fake.time_object()),
                "device_type": random.choice(["iOS", "Android", "Web"]),
                "location": fake.city(),
                "event_date": event_date
            })

    import pandas as pd
    from pyspark.sql.functions import col, to_timestamp, to_date

    df = pd.DataFrame(rows)
    spark_df = spark.createDataFrame(df)

    spark_df = (
        spark_df
        .withColumn("timestamp", to_timestamp("timestamp"))
        .withColumn("event_date", to_date("event_date"))
    )

    spark_df.write.mode("append").format("delta").saveAsTable("gameanalytics.raw_logins")


# COMMAND ----------

def generate_sessions(users, event_date):
    rows = []
    for user in users:
        for _ in range(random.randint(0, 3)):
            start = datetime.combine(event_date, fake.time_object())
            end = start + timedelta(minutes=random.randint(5, 60))
            rows.append({
                "session_id": str(uuid.uuid4()),
                "user_id": user.user_id,
                "game_type": random.choice(["Arcade", "Ranked", "Practice"]),
                "start_time": start,
                "end_time": end,
                "score": random.randint(0, 1000),
                "result": random.choice(["Win", "Lose"]),
                "event_date": event_date
            })

    import pandas as pd
    from pyspark.sql.functions import col, to_timestamp, to_date

    df = pd.DataFrame(rows)
    spark_df = spark.createDataFrame(df)
    spark_df = (
        spark_df
        .withColumn("score", col("score").cast("int"))
        .withColumn("start_time", to_timestamp("start_time"))
        .withColumn("end_time", to_timestamp("end_time"))
        .withColumn("event_date", to_date("event_date"))
    )

    spark_df.write.mode("append").format("delta").saveAsTable("gameanalytics.raw_game_sessions")


# COMMAND ----------

def generate_purchases(users, event_date):
    rows = []
    for user in users:
        if random.random() < 0.2:
            rows.append({
                "purchase_id": str(uuid.uuid4()),
                "user_id": user.user_id,
                "item": random.choice(["Skin", "Coins", "Boost", "Pass"]),
                "amount": round(random.uniform(0.99, 49.99), 2),
                "timestamp": datetime.combine(event_date, fake.time_object()),
                "event_date": event_date
            })

    import pandas as pd
    from pyspark.sql.functions import col, to_timestamp, to_date

    df = pd.DataFrame(rows)
    spark_df = spark.createDataFrame(df)

    spark_df = (
        spark_df
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("timestamp", to_timestamp("timestamp"))
        .withColumn("event_date", to_date("event_date"))
    )

    spark_df.write.mode("append").format("delta").saveAsTable("gameanalytics.raw_purchases")


# COMMAND ----------

def generate_leaderboard(users, event_date):
    rows = []
    for game in ["Arcade", "Ranked", "Practice"]:
        sampled_users = random.sample(users, min(20, len(users)))
        for rank, user in enumerate(sampled_users, 1):
            rows.append({
                "user_id": user.user_id,
                "game_type": game,
                "rank": rank,
                "event_date": event_date
            })

    import pandas as pd
    from pyspark.sql.functions import col, to_date

    df = pd.DataFrame(rows)
    spark_df = spark.createDataFrame(df)

    spark_df = (
        spark_df
        .withColumn("rank", col("rank").cast("int"))
        .withColumn("event_date", to_date("event_date"))
    )

    spark_df.write.mode("append").format("delta").saveAsTable("gameanalytics.raw_leaderboard_snapshots")


# COMMAND ----------

from datetime import date

def run_daily_generation(run_date):
    generate_users()
    users = get_users()
    generate_logins(users, run_date)
    generate_sessions(users, run_date)
    generate_purchases(users, run_date)
    generate_leaderboard(users, run_date)


# COMMAND ----------

run_daily_generation(run_date)