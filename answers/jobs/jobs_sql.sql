-- Databricks notebook source
-- DBTITLE 1,Make sure to use our catalog and schema
USE CATALOG dtdemos;
USE SCHEMA retail_c360;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC # Ingesting into Bronze.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Ingest raw app events stream in incremental mode
CREATE TABLE IF NOT EXISTS churn_app_events_bronze
COMMENT "Application events and sessions."
AS SELECT * FROM read_files("<your_volume_root_path>/events", 
format => "csv", 
inferSchema => "true", 
schemaLocation => "<your_volume_root_path>/spark_schemas_sql/churn_app_events_bronze"
)

-- COMMAND ----------

-- DBTITLE 1,Ingest raw order data
CREATE TABLE IF NOT EXISTS churn_orders_bronze
COMMENT "Spending score from raw data."
AS SELECT * FROM read_files("<your_volume_root_path>/orders", 
format => "json", 
inferSchema => "true", 
schemaLocation => "<your_volume_root_path>/spark_schemas_sql/churn_orders_bronze"
)
WHERE _rescued_data IS NULL

-- COMMAND ----------

-- DBTITLE 1,Ingest raw user data
CREATE TABLE IF NOT EXISTS churn_users_bronze
COMMENT "Raw user data coming from json files ingested in incremental with Auto Loader to support schema inference and evolution."
AS SELECT * FROM read_files("<your_volume_root_path>/users", 
format => "json", 
inferSchema => "true", 
schemaLocation => "<your_volume_root_path>/spark_schemas_sql/churn_users_bronze"
)
WHERE _rescued_data IS NULL

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC # Standing up our tables in Silver.

-- COMMAND ----------

-- DBTITLE 1,Stand up our silver app events table, filtering for invalid records
CREATE TABLE IF NOT EXISTS churn_app_events_silver
COMMENT "App events with Data Quality constraints enforced & metadata enriched."
AS SELECT
  *
FROM churn_app_events_bronze
WHERE user_id IS NOT NULL AND event_id IS NOT NULL AND _rescued_data IS NULL

-- COMMAND ----------

-- DBTITLE 1,Stand up our silver orders table, filtering for invalid records
CREATE TABLE IF NOT EXISTS churn_orders_silver
COMMENT "Order data cleaned and anonymized for analysis."
AS SELECT
  cast(amount as int),
  id as order_id,
  user_id,
  cast(item_count as int),
  to_timestamp(transaction_date, "MM-dd-yyyy HH:mm:ss") as creation_date
FROM churn_orders_bronze
WHERE id IS NOT NULL AND user_id IS NOT NULL

-- COMMAND ----------

-- DBTITLE 1,Anonymise our users table and cluster it by id
CREATE TABLE IF NOT EXISTS churn_users_silver CLUSTER BY (user_id)
COMMENT "User data cleaned and anonymized for analysis."
AS SELECT
  id as user_id,
  ai_mask(email, array("email")) as email, -- Alternatively, sha01(email)
  to_timestamp(creation_date, "MM-dd-yyyy HH:mm:ss") as creation_date, 
  to_timestamp(last_activity_date, "MM-dd-yyyy HH:mm:ss") as last_activity_date, 
  ai_mask(firstname, array("name")) as firstname, -- Alternatively, "*****" as firstname
  ai_mask(lastname, array("name")) as lastname, -- Alternatively, "*****" as lastname
  ai_mask(address, array("address")) as address, --Alternatively, sha01(address)
  canal, 
  country,
  gender,
  age_group, 
  churn
FROM churn_users_bronze
WHERE id IS NOT NULL

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC # Standing up our final Gold table.

-- COMMAND ----------

-- DBTITLE 1,Create our final gold table
CREATE TABLE IF NOT EXISTS customer_churn_gold
COMMENT "Final user table with all information for Analysis & ML."
AS 
  WITH 
    churn_orders_stats AS (SELECT user_id, count(*) as order_count, sum(amount) as total_amount, sum(item_count) as total_item, max(creation_date) as last_transaction
      FROM churn_orders_silver GROUP BY user_id),  
    churn_app_events_stats as (
      SELECT first(platform) as platform, user_id, count(*) as event_count, count(distinct session_id) as session_count, max(to_timestamp(date, "MM-dd-yyyy HH:mm:ss")) as last_event
        FROM live.churn_app_events_silver_dlt GROUP BY user_id)

  SELECT *, 
         datediff(now(), creation_date) as days_since_creation,
         datediff(now(), last_activity_date) as days_since_last_activity,
         datediff(now(), last_event) as days_last_event
       FROM churn_users_silver
         INNER JOIN churn_orders_stats using (user_id)
         INNER JOIN churn_app_events_stats using (user_id)