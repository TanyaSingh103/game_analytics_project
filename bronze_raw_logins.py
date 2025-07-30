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

users = spark.table("gameanalytics.raw_users").select("user_id").collect()

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

generate_logins(users, run_date)