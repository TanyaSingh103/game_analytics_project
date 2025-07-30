# Databricks notebook source

from pyspark.sql.functions import col, lit
from pyspark.sql.types import LongType, DoubleType, StringType
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import pandas as pd

# Load gold_user_demographics
df = spark.table("gameanalytics.gold_user_demographics")

# Separate inactive users
inactive_df = df.filter((col("total_sessions") < 3) & (col("total_spent") == 0))
active_df = df.subtract(inactive_df)

# Convert active users to pandas
active_pd = active_df.select(
    "user_id", "gender", "age_bracket", "country",
    "total_sessions", "avg_session_duration", "total_spent", "num_purchases"
).toPandas()

# Standardize features
scaler = StandardScaler()
features = ["total_sessions", "avg_session_duration", "total_spent", "num_purchases"]
X_scaled = scaler.fit_transform(active_pd[features])

# KMeans clustering
kmeans = KMeans(n_clusters=3, random_state=42)
active_pd["cluster"] = kmeans.fit_predict(X_scaled)

# Compute mean metrics for each cluster
cluster_metrics = active_pd.groupby("cluster")[
    ["total_spent", "avg_session_duration"]
].mean()

# Identify cluster labels
whale_cluster = cluster_metrics["total_spent"].idxmax()
hardcore_cluster = cluster_metrics.drop(index=whale_cluster)["avg_session_duration"].idxmax()
casual_cluster = list(set([0, 1, 2]) - {whale_cluster, hardcore_cluster})[0]

# Map clusters to labels
cluster_to_label = {
    whale_cluster: "Whale",
    hardcore_cluster: "Hardcore",
    casual_cluster: "Casual"
}
active_pd["user_segment"] = active_pd["cluster"].map(cluster_to_label)

# Convert back to Spark
active_clustered_spark = spark.createDataFrame(active_pd)

# Add matching column to inactive users
inactive_tagged_df = inactive_df.withColumn("user_segment", lit("Inactive"))

# Align column order explicitly
final_df = active_clustered_spark.select(
    "user_id", "gender", "age_bracket", "country",
    "total_sessions", "avg_session_duration", "total_spent", "num_purchases", "user_segment"
).unionByName(
    inactive_tagged_df.select(
        "user_id", "gender", "age_bracket", "country",
        "total_sessions", "avg_session_duration", "total_spent", "num_purchases", "user_segment"
    )
)

# Write to Delta
final_df.write.mode("overwrite").format("delta").saveAsTable("gameanalytics.gold_user_segments")

# COMMAND ----------

# display(spark.table("gameanalytics.gold_user_segments").groupBy("user_segment").count())


# COMMAND ----------

# from pyspark.sql.functions import avg, count, round as spark_round

# cluster_profiles = final_df.groupBy("user_segment").agg(
#     spark_round(avg("total_sessions"), 2).alias("avg_sessions"),
#     spark_round(avg("avg_session_duration"), 2).alias("avg_duration"),
#     spark_round(avg("total_spent"), 2).alias("avg_spent"),
#     spark_round(avg("num_purchases"), 2).alias("avg_purchases"),
#     count("user_id").alias("count")
# )

# display(cluster_profiles.orderBy("user_segment"))

# COMMAND ----------

# %sql
# select * from gameanalytics.gold_user_segments 