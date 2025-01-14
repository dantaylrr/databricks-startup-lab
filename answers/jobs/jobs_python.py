# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Set globals.

# COMMAND ----------

# DBTITLE 1,Init global variables
catalog = "<your_catalog>"
schema = "<your_schema>"
volume_folder = f"/Volumes/{catalog}/{schema}/c360"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Ingesting into Bronze.

# COMMAND ----------

# DBTITLE 1,Ingest all of our data from our source volumes
import pyspark.sql.functions as F

def ingest_from_volume(source, sink, checkpoint_location, **cloudfiles_config):
  """
  Scalable method for ingestion of UC volumes, demonstrative example
  """
  raw_table = (spark.readStream
                              .format("cloudFiles")
                              .options(**cloudfiles_config) # Unpack cloudfiles options
                              .option("cloudFiles.inferColumnTypes", "true")
                              .load(source))

  return (raw_table.writeStream
                    .option("checkpointLocation", f"{checkpoint_location}") # Exactly once delivery on Delta tables over restart/kill
                    .option("mergeSchema", "true") # Merge any new column dynamically
                    .trigger(availableNow = True) # Remove for real time streaming
                    .toTable(f'{sink}')) # Table destination

# COMMAND ----------

# DBTITLE 1,Define our table configs
tables = [
{"source": "events", "table_name": "spark_churn_app_events_bronze", "cloudfiles_config": {"cloudFiles.format": "csv", "cloudFiles.schemaLocation": f"{volume_folder}/spark_schemas/spark_churn_app_events_bronze"}},
{"source": "orders", "table_name": "spark_churn_orders_bronze", "cloudfiles_config": {"cloudFiles.format": "json", "cloudFiles.schemaLocation": f"{volume_folder}/spark_schemas/spark_churn_orders_bronze"}},
{"source": "users", "table_name": "spark_churn_users_bronze", "cloudfiles_config": {"cloudFiles.format": "json", "cloudFiles.schemaLocation": f"{volume_folder}/spark_schemas/spark_churn_users_bronze"}},
]

# COMMAND ----------

# DBTITLE 1,Orchestrate ingestion
for table in tables:
  ingest_from_volume(source=f'{volume_folder}/{table["source"]}', sink=f'{catalog}.{schema}.{table["table_name"]}', checkpoint_location=f'{volume_folder}/spark_checkpoints/{table["table_name"]}', **table["cloudfiles_config"]).awaitTermination()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Standing up our tables in Silver.

# COMMAND ----------

# DBTITLE 1,Clean app events data
(spark.readStream
  .table(f'{catalog}.{schema}.spark_churn_app_events_bronze')
  .filter(F.col('user_id').isNotNull())
  .filter(F.col('event_id').isNotNull())
  .filter(F.col('_rescued_data').isNull())
  .drop(F.col("_rescued_data"))
  .writeStream
  .option("checkpointLocation", f"{volume_folder}/spark_checkpoints/spark_churn_app_events_silver")
  .option("mergeSchema", "true")
  .trigger(availableNow = True)
  .table(f"{catalog}.{schema}.spark_churn_app_events_silver").awaitTermination()
  )

# COMMAND ----------

# DBTITLE 1,Clean order data
(spark.readStream
  .table(f'{catalog}.{schema}.spark_churn_orders_bronze')
  .filter(F.col('id').isNotNull())
  .filter(F.col('user_id').isNotNull())
  .filter(F.col('_rescued_data').isNull())
  .select(F.col("amount").cast("int").alias("amount"),
          F.col("id").alias("order_id"),
          F.col("user_id"),
          F.col("item_count").cast("int").alias("item_count"),
          F.to_timestamp(F.col("transaction_date"), "MM-dd-yyyy HH:mm:ss").alias("creation_date"))
  .writeStream
  .option("checkpointLocation", f"{volume_folder}/spark_checkpoints/spark_churn_orders_silver")
  .option("mergeSchema", "true")
  .trigger(availableNow = True)
  .table(f"{catalog}.{schema}.spark_churn_orders_silver").awaitTermination()
  )

# COMMAND ----------

# DBTITLE 1,Clean and anonymise user data
(spark.readStream 
  .table(f"{catalog}.{schema}.spark_churn_users_bronze")
  .filter(F.col('id').isNotNull())
  .filter(F.col('_rescued_data').isNull())
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
          F.col("churn").cast("int").alias("churn"))
  .writeStream
    .option("checkpointLocation", f"{volume_folder}/spark_checkpoints/spark_churn_users_silver")
    .option("mergeSchema", "true")
    .trigger(availableNow = True)
    .table(f"{catalog}.{schema}.spark_churn_users_silver").awaitTermination()
    )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Standing up our final Gold table.

# COMMAND ----------

# DBTITLE 1,Create our gold view
churn_app_events_stats_df = (spark.read
                             .table(f"{catalog}.{schema}.spark_churn_app_events_silver")
                             .groupby("user_id")
                             .agg(F.first("platform").alias("platform"),
                                F.count('*').alias("event_count"),
                                F.count_distinct("session_id").alias("session_count"),
                                F.max(F.to_timestamp("date", "MM-dd-yyyy HH:mm:ss")).alias("last_event"))
                             )
  
churn_orders_stats_df = (spark.read
                         .table(f"{catalog}.{schema}.spark_churn_orders_silver")
                         .groupby("user_id")
                         .agg(F.count('*').alias("order_count"),
                              F.sum("amount").alias("total_amount"),
                              F.sum("item_count").alias("total_item"),
                              F.max("creation_date").alias("last_transaction"))
                         )
  
(spark.read
 .table(f"{catalog}.{schema}.spark_churn_users_silver")
 .join(churn_app_events_stats_df, on="user_id")
 .join(churn_orders_stats_df, on="user_id")
 .withColumn("days_since_creation", F.datediff(F.current_timestamp(), F.col("creation_date")))
 .withColumn("days_since_last_activity", F.datediff(F.current_timestamp(), F.col("last_activity_date")))
 .withColumn("days_last_event", F.datediff(F.current_timestamp(), F.col("last_event")))
 .write
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.spark_customer_churn_gold")
 )