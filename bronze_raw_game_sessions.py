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
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

fake = Faker()
spark = SparkSession.builder.getOrCreate()


# COMMAND ----------

users = spark.table("gameanalytics.raw_users").select("user_id").collect()

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

generate_sessions(users, run_date)