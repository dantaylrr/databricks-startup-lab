# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Ingesting into Bronze.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Ingest raw app events stream in incremental mode
import dlt
from pyspark.sql import functions as F

@dlt.create_table(comment="Application events and sessions.")
@dlt.expect("App events correct schema", "_rescued_data IS NULL")
def spark_churn_app_events_bronze_dlt():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("<your_volume_root_path>/events"))

# COMMAND ----------

# DBTITLE 1,Ingest raw order data
@dlt.create_table(comment="Spending score from raw data.")
@dlt.expect("Orders correct schema", "_rescued_data IS NULL")
def spark_churn_orders_bronze_dlt():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("<your_volume_root_path>/orders"))

# COMMAND ----------

# DBTITLE 1,Ingest raw user data
@dlt.create_table(comment="Raw user data coming from json files ingested in incremental with Auto Loader to support schema inference and evolution.")
@dlt.expect("Users correct schema", "_rescued_data IS NULL")
def spark_churn_users_bronze_dlt():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("<your_volume_root_path>/users"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Standing up our tables in Silver.

# COMMAND ----------

# DBTITLE 1,Clean app events data
@dlt.create_table(comment="App events with Data Quality constraints enforced & metadata enriched.")
@dlt.expect_or_drop("user_valid_id", "user_id IS NOT NULL")
@dlt.expect_or_drop("event_valid_id", "event_id IS NOT NULL")
def spark_churn_app_events_silver_dlt():
  return (dlt
          .read_stream("spark_churn_app_events_bronze_dlt"))

# COMMAND ----------

# DBTITLE 1,Clean order data
@dlt.create_table(comment="Order data cleaned and anonymized for analysis.")
@dlt.expect_or_drop("order_valid_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("order_valid_user_id", "user_id IS NOT NULL")
def spark_churn_orders_silver_dlt():
  return (dlt
          .read_stream("spark_churn_orders_bronze_dlt")
          .select(F.col("amount").cast("int").alias("amount"),
                  F.col("id").alias("order_id"),
                  F.col("user_id"),
                  F.col("item_count").cast("int").alias("item_count"),
                  F.to_timestamp(F.col("transaction_date"), "MM-dd-yyyy HH:mm:ss").alias("creation_date"))
         )

# COMMAND ----------

# DBTITLE 1,Clean and anonymise user data
@dlt.create_table(comment="User data cleaned and anonymized for analysis.")
@dlt.expect_or_drop("user_valid_id", "user_id IS NOT NULL")
def spark_churn_users_silver_dlt():
  return (dlt
          .read_stream("spark_churn_users_bronze_dlt")
          .select(F.col("id").alias("user_id"),
                  F.sha1(F.col("email")).alias("email"), 
                  F.to_timestamp(F.col("creation_date"), "MM-dd-yyyy HH:mm:ss").alias("creation_date"), 
                  F.to_timestamp(F.col("last_activity_date"), "MM-dd-yyyy HH:mm:ss").alias("last_activity_date"), 
                  F.lit("*****").alias("firstname"), 
                  F.lit("*****").alias("lastname"), 
                  F.sha1(F.col("address")).alias("address"),
                  F.col("canal"),
                  F.col("country"),
                  F.col("gender").cast("int").alias("gender"),
                  F.col("age_group").cast("int").alias("age_group"), 
                  F.col("churn").cast("int").alias("churn")))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Standing up our final Gold table.

# COMMAND ----------

# DBTITLE 1,Create our gold view
@dlt.create_table(comment="Final user table with all information for Analysis / ML.")
def spark_customer_churn_gold_dlt():
  churn_app_events_stats_df = (dlt
          .read("spark_churn_app_events_silver_dlt")
          .groupby("user_id")
          .agg(F.first("platform").alias("platform"),
               F.count('*').alias("event_count"),
               F.count_distinct("session_id").alias("session_count"),
               F.max(F.to_timestamp("date", "MM-dd-yyyy HH:mm:ss")).alias("last_event"))
                              )
  
  churn_orders_stats_df = (dlt
          .read("spark_churn_orders_silver_dlt")
          .groupby("user_id")
          .agg(F.count('*').alias("order_count"),
               F.sum("amount").alias("total_amount"),
               F.sum("item_count").alias("total_item"),
               F.max("creation_date").alias("last_transaction"))
         )
  
  return (dlt
          .read("spark_churn_users_silver_dlt")
          .join(churn_app_events_stats_df, on="user_id")
          .join(churn_orders_stats_df, on="user_id")
          .withColumn("days_since_creation", F.datediff(F.current_timestamp(), F.col("creation_date")))
          .withColumn("days_since_last_activity", F.datediff(F.current_timestamp(), F.col("last_activity_date")))
          .withColumn("days_last_event", F.datediff(F.current_timestamp(), F.col("last_event")))
         )