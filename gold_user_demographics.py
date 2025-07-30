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

# from pyspark.sql.functions import col, lit
# from sklearn.preprocessing import StandardScaler
# from sklearn.cluster import KMeans
# import pandas as pd

# # Load data from view
# df = spark.sql("SELECT * FROM gold_user_demographics_view")

# # Split users
# inactive_df = df.filter(((col("total_sessions") < 4) | (col("avg_session_duration") < 10)) & (col("total_spent") == 0))
# active_df = df.subtract(inactive_df)

# # Convert to pandas for clustering
# active_pd = active_df.select(
#     "user_id", "gender", "age_bracket", "country",
#     "total_sessions", "avg_session_duration", "total_spent", "num_purchases"
# ).toPandas()

# # KMeans
# scaler = StandardScaler()
# features = ["total_sessions", "avg_session_duration", "total_spent", "num_purchases"]
# X_scaled = scaler.fit_transform(active_pd[features])

# kmeans = KMeans(n_clusters=3, random_state=42)
# active_pd["cluster"] = kmeans.fit_predict(X_scaled)

# # Assign user_segment label
# cluster_metrics = active_pd.groupby("cluster")[["total_spent", "avg_session_duration"]].mean()
# whale_cluster = cluster_metrics["total_spent"].idxmax()
# hardcore_cluster = cluster_metrics.drop(index=whale_cluster)["avg_session_duration"].idxmax()
# casual_cluster = list(set([0, 1, 2]) - {whale_cluster, hardcore_cluster})[0]

# label_map = {whale_cluster: "Whale", hardcore_cluster: "Hardcore", casual_cluster: "Casual"}
# active_pd["user_segment"] = active_pd["cluster"].map(label_map)

# # Convert back to Spark
# active_clustered_spark = spark.createDataFrame(active_pd.drop(columns=["cluster"]))

# # Inactive users
# inactive_tagged_df = inactive_df.withColumn("user_segment", lit("Inactive"))

# # Merge
# final_df = active_clustered_spark.unionByName(inactive_tagged_df)

# # Save final result to gold_user_demographics (overwrite or upsert)
# final_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gameanalytics.gold_user_demographics")



# COMMAND ----------

# from pyspark.ml.feature import VectorAssembler, StandardScaler
# from pyspark.ml.clustering import KMeans
# from pyspark.sql.functions import col, lit, when

# # Load gold view
# df = spark.sql("SELECT * FROM gold_user_demographics_view")

# # Inactive users = low activity and zero purchase
# inactive_df = df.filter(
#     ((col("total_sessions") < 4) | (col("avg_session_duration") < 10)) & (col("total_spent") == 0)
# )
# active_df = df.subtract(inactive_df)

# # Step 1: Assemble feature vector
# features = ["total_sessions", "avg_session_duration", "total_spent", "num_purchases"]
# vec_assembler = VectorAssembler(inputCols=features, outputCol="features_vec")
# assembled_df = vec_assembler.transform(active_df)

# # Step 2: Standardize features
# scaler = StandardScaler(inputCol="features_vec", outputCol="scaled_features", withMean=True, withStd=True)
# scaler_model = scaler.fit(assembled_df)
# scaled_df = scaler_model.transform(assembled_df)

# # Step 3: Apply KMeans
# kmeans = KMeans(featuresCol="scaled_features", predictionCol="cluster", k=3, seed=42)
# kmeans_model = kmeans.fit(scaled_df)
# clustered_df = kmeans_model.transform(scaled_df)

# # Step 4: Find metrics per cluster
# cluster_avg = clustered_df.groupBy("cluster").agg(
#     {"total_spent": "avg", "avg_session_duration": "avg"}
# ).withColumnRenamed("avg(total_spent)", "avg_spent") \
#  .withColumnRenamed("avg(avg_session_duration)", "avg_duration")

# # Step 5: Identify cluster labels
# whale_cluster = cluster_avg.orderBy(col("avg_spent").desc()).first()["cluster"]
# hardcore_cluster = cluster_avg.filter(col("cluster") != whale_cluster).orderBy(col("avg_duration").desc()).first()["cluster"]
# casual_cluster = list(set([0, 1, 2]) - {whale_cluster, hardcore_cluster})[0]

# # Step 6: Assign segment labels
# labeled_df = clustered_df.withColumn("user_segment", when(col("cluster") == whale_cluster, "Whale")
#                                      .when(col("cluster") == hardcore_cluster, "Hardcore")
#                                      .when(col("cluster") == casual_cluster, "Casual"))

# # Step 7: Drop extra columns and reassemble
# final_active_df = labeled_df.drop("features_vec", "scaled_features", "cluster")

# # Step 8: Tag inactive users
# inactive_labeled_df = inactive_df.withColumn("user_segment", lit("Inactive"))

# # Step 9: Merge and save
# final_df = final_active_df.unionByName(inactive_labeled_df)

# final_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gameanalytics.gold_user_demographics")


# COMMAND ----------

from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col, lit, when
import mlflow
import mlflow.spark
from mlflow.models.signature import infer_signature
from pyspark.sql import SparkSession
from pyspark.sql.functions import datediff, current_date

# Unity Catalog workaround
import mlflow.tracking._model_registry.utils
mlflow.tracking._model_registry.utils._get_registry_uri_from_spark_session = lambda: "databricks-uc"

# Set experiment
mlflow.set_experiment("/Users/tanya.singh@eucloid.com/Notebooks/game_analytics_project/gold_user_demographics")

with mlflow.start_run(run_name="User_Segmentation_KMeans") as run:
    # Load view
    df = spark.sql("SELECT * FROM gold_user_demographics_view")

    
    # Step 2: Identify inactive users (zero purchases + low activity)
    df = df.withColumn("user_age_days", datediff(current_date(), col("registration_date")))

    inactive_df = df.filter(
        (col("total_spent") == 0) &
        (
            ((col("total_sessions") / col("user_age_days")) < 0.25) |         # Less than 1 session every 5 days
            (col("avg_session_duration") < 15)                               # Avg session duration < 5 min
        )
    )
    active_df = df.subtract(inactive_df)

    # Assemble + scale
    features = ["total_sessions", "avg_session_duration", "total_spent", "num_purchases"]
    vec_assembler = VectorAssembler(inputCols=features, outputCol="features_vec")
    assembled_df = vec_assembler.transform(active_df)

    scaler = StandardScaler(inputCol="features_vec", outputCol="scaled_features", withMean=True, withStd=True)
    scaler_model = scaler.fit(assembled_df)
    scaled_df = scaler_model.transform(assembled_df)

    # Train KMeans
    kmeans = KMeans(featuresCol="scaled_features", predictionCol="cluster", k=3, seed=42)
    kmeans_model = kmeans.fit(scaled_df)
    clustered_df = kmeans_model.transform(scaled_df)

    # Cluster labeling
    cluster_avg = clustered_df.groupBy("cluster").agg({"total_spent": "avg", "avg_session_duration": "avg"}) \
        .withColumnRenamed("avg(total_spent)", "avg_spent") \
        .withColumnRenamed("avg(avg_session_duration)", "avg_duration")

    whale_cluster = cluster_avg.orderBy(col("avg_spent").desc()).first()["cluster"]
    hardcore_cluster = cluster_avg.filter(col("cluster") != whale_cluster).orderBy(col("avg_duration").desc()).first()["cluster"]
    casual_cluster = list(set([0, 1, 2]) - {whale_cluster, hardcore_cluster})[0]

    labeled_df = clustered_df.withColumn("user_segment", when(col("cluster") == whale_cluster, "Whale")
                                         .when(col("cluster") == hardcore_cluster, "Hardcore")
                                         .otherwise("Casual"))

    # Finalize
    final_active_df = labeled_df.drop("features_vec", "scaled_features", "cluster")
    inactive_labeled_df = inactive_df.withColumn("user_segment", lit("Inactive"))
    final_df = final_active_df.unionByName(inactive_labeled_df)

    # Save to Gold table
    final_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gameanalytics.gold_user_demographics")

    # Log model
    mlflow.log_param("features", ", ".join(features))
    mlflow.log_param("model", "KMeans")

    from mlflow.models.signature import infer_signature

    # Signature inference
    sample_input = scaled_df.select("scaled_features").limit(5).toPandas()
    sample_output = clustered_df.select("cluster").limit(5).toPandas()
    signature = infer_signature(sample_input, sample_output)

    # Log model with signature
    mlflow.spark.log_model(kmeans_model, "user_segmentation_model", signature=signature)


    # Register model
    run_id = run.info.run_id
    model_name = "game-analytics-user-segmentation"
    registered_model = mlflow.register_model(f"runs:/{run_id}/user_segmentation_model", model_name)

    client = mlflow.tracking.MlflowClient()
    client.set_registered_model_alias(model_name, "Production", registered_model.version)

# COMMAND ----------

