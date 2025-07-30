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

generate_purchases(users, run_date)