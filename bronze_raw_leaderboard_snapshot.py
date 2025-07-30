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

generate_leaderboard(users, run_date)