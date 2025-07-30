# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW gold_user_demographics_view AS
# MAGIC WITH session_metrics AS (
# MAGIC   SELECT
# MAGIC     user_id,
# MAGIC     COUNT(*) AS total_sessions,
# MAGIC     ROUND(AVG(session_duration_minutes), 2) AS avg_session_duration
# MAGIC   FROM gameanalytics.silver_user_sessions
# MAGIC   GROUP BY user_id
# MAGIC ),
# MAGIC purchase_metrics AS (
# MAGIC   SELECT
# MAGIC     user_id,
# MAGIC     ROUND(SUM(amount), 2) AS total_spent,
# MAGIC     COUNT(*) AS num_purchases
# MAGIC   FROM gameanalytics.silver_purchases_enriched
# MAGIC   GROUP BY user_id
# MAGIC )
# MAGIC SELECT
# MAGIC   u.user_id,
# MAGIC   u.gender,
# MAGIC   CASE 
# MAGIC     WHEN u.age BETWEEN 13 AND 17 THEN '13-17'
# MAGIC     WHEN u.age BETWEEN 18 AND 25 THEN '18-25'
# MAGIC     WHEN u.age BETWEEN 26 AND 35 THEN '26-35'
# MAGIC     WHEN u.age BETWEEN 36 AND 45 THEN '36-45'
# MAGIC     ELSE '45+'
# MAGIC   END AS age_bracket,
# MAGIC   u.country,
# MAGIC   u.signup_date AS registration_date,
# MAGIC   COALESCE(s.total_sessions, 0) AS total_sessions,
# MAGIC   COALESCE(s.avg_session_duration, 0) AS avg_session_duration,
# MAGIC   COALESCE(p.total_spent, 0.0) AS total_spent,
# MAGIC   COALESCE(p.num_purchases, 0) AS num_purchases
# MAGIC FROM gameanalytics.raw_users u
# MAGIC LEFT JOIN session_metrics s ON u.user_id = s.user_id
# MAGIC LEFT JOIN purchase_metrics p ON u.user_id = p.user_id;
# MAGIC

# COMMAND ----------

import mlflow
mlflow.autolog(disable=True)  

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col, lit, when
import mlflow.spark


# Step 1: Load the gold view
df = spark.sql("SELECT * FROM gold_user_demographics_view")

# Step 2: Identify inactive users (zero purchases + low activity)
from pyspark.sql.functions import datediff, current_date, col

df = df.withColumn("user_age_days", datediff(current_date(), col("registration_date")))

inactive_df = df.filter(
    (col("total_spent") == 0) &
    (
        ((col("total_sessions") / col("user_age_days")) < 0.25) |         # Less than 1 session every 5 days
        (col("avg_session_duration") < 5)                               # Avg session duration < 5 min
    )
)

active_df = df.subtract(inactive_df)

# Step 3: Assemble and scale features
features = ["total_sessions", "avg_session_duration", "total_spent", "num_purchases"]
vec_assembler = VectorAssembler(inputCols=features, outputCol="features_vec")
assembled_df = vec_assembler.transform(active_df)

scaler = StandardScaler(inputCol="features_vec", outputCol="scaled_features", withMean=True, withStd=True)
scaler_model = scaler.fit(assembled_df)  # Still needed locally to scale features
scaled_df = scaler_model.transform(assembled_df)

# Step 4: Load the registered KMeans model (from Production)
model_name = "db_certification.default.game-analytics-user-segmentation"
kmeans_model = mlflow.spark.load_model(f"models:/{model_name}@Production")

# Step 5: Predict clusters
predicted_df = kmeans_model.transform(scaled_df)

# Step 6: Label clusters based on avg_spent and avg_duration
cluster_avg = predicted_df.groupBy("cluster").agg(
    {"total_spent": "avg", "avg_session_duration": "avg"}
).withColumnRenamed("avg(total_spent)", "avg_spent") \
 .withColumnRenamed("avg(avg_session_duration)", "avg_duration")

whale_cluster = cluster_avg.orderBy(col("avg_spent").desc()).first()["cluster"]
hardcore_cluster = cluster_avg.filter(col("cluster") != whale_cluster).orderBy(col("avg_duration").desc()).first()["cluster"]
casual_cluster = list(set([0, 1, 2]) - {whale_cluster, hardcore_cluster})[0]

# Step 7: Tag segment labels
labeled_df = predicted_df.withColumn("user_segment", when(col("cluster") == whale_cluster, "Whale")
                                     .when(col("cluster") == hardcore_cluster, "Hardcore")
                                     .otherwise("Casual"))

# Step 8: Merge with inactive users and save to Gold
final_active_df = labeled_df.drop("features_vec", "scaled_features", "cluster")
inactive_labeled_df = inactive_df.withColumn("user_segment", lit("Inactive"))
final_df = final_active_df.unionByName(inactive_labeled_df)

final_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gameanalytics.gold_user_demographics")


# COMMAND ----------

# from pyspark.ml.evaluation import ClusteringEvaluator

# evaluator = ClusteringEvaluator(
#     predictionCol="cluster",            
#     featuresCol="scaled_features",      # same column used in training
#     metricName="silhouette"
# )

# silhouette_score = evaluator.evaluate(predicted_df)
# print("Silhouette Score:", silhouette_score)


# COMMAND ----------

# from pyspark.ml.clustering import KMeans
# from pyspark.ml.evaluation import ClusteringEvaluator

# scores = []
# evaluator = ClusteringEvaluator(featuresCol="scaled_features", metricName="silhouette", distanceMeasure="squaredEuclidean")

# for k in range(2, 9):
#     kmeans = KMeans(featuresCol="scaled_features", k=k, seed=42)
#     model = kmeans.fit(scaled_df)
#     predictions = model.transform(scaled_df)
#     score = evaluator.evaluate(predictions)
#     scores.append((k, score))
#     print(f"k={k}, Silhouette Score={score}")


# COMMAND ----------

# import pandas as pd
# import matplotlib.pyplot as plt

# pdf = pd.DataFrame(scores, columns=["k", "silhouette"])
# plt.plot(pdf["k"], pdf["silhouette"], marker='o')
# plt.xlabel("k (number of clusters)")
# plt.ylabel("Silhouette Score")
# plt.title("Elbow Method using Silhouette Score")
# plt.grid(True)
# plt.show()
