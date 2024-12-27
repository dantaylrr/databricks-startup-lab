-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC # Ingesting into Bronze.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Ingest raw app events stream in incremental mode
CREATE STREAMING TABLE churn_app_events_bronze_dlt (
  CONSTRAINT events_correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "Application events and sessions."
AS SELECT * FROM STREAM read_files("<your_volume_root_path>/events", 
format => "csv", 
inferSchema => "true"
)

-- COMMAND ----------

-- DBTITLE 1,Ingest raw order data
CREATE STREAMING TABLE churn_orders_bronze_dlt (
  CONSTRAINT orders_correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "Spending score from raw data."
AS SELECT * FROM STREAM read_files("<your_volume_root_path>/orders",
format => "json",
inferSchema => "true"
)

-- COMMAND ----------

-- DBTITLE 1,Ingest raw user data
CREATE STREAMING TABLE churn_users_bronze_dlt (
  CONSTRAINT users_correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "Raw user data coming from json files ingested in incremental with Auto Loader to support schema inference and evolution."
AS SELECT * FROM STREAM read_files("<your_volume_root_path>/users", 
format => "json", 
inferSchema => "true"
)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC # Standing up our tables in Silver.

-- COMMAND ----------

-- DBTITLE 1,Add data quality constraints to our app events data
CREATE STREAMING TABLE churn_app_events_silver_dlt (
  CONSTRAINT user_valid_id EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW, -- Drop invalid user_ids
  CONSTRAINT event_valid_id EXPECT (event_id IS NOT NULL) ON VIOLATION DROP ROW -- Drop invalid event_ids
)
COMMENT "App events with Data Quality constraints enforced & metadata enriched."
AS SELECT
  *
FROM STREAM(live.churn_app_events_bronze_dlt)

-- COMMAND ----------

-- DBTITLE 1,Add data quality constraints to our order data
CREATE STREAMING TABLE churn_orders_silver_dlt (
  CONSTRAINT order_valid_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW, -- Just use valid orders 
  CONSTRAINT order_valid_user_id EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW -- Just use valid orders
)
COMMENT "Order data cleaned and anonymized for analysis."
AS SELECT
  cast(amount as int),
  id as order_id,
  user_id,
  cast(item_count as int),
  to_timestamp(transaction_date, "MM-dd-yyyy HH:mm:ss") as creation_date
FROM STREAM(live.churn_orders_bronze_dlt)

-- COMMAND ----------

-- DBTITLE 1,Add data quality constraints to our users data
CREATE STREAMING TABLE churn_users_silver_dlt (
  CONSTRAINT user_valid_id EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW -- Just use valid users / customers
)
TBLPROPERTIES (pipelines.autoOptimize.zOrderCols = "user_id")
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
FROM STREAM(live.churn_users_bronze_dlt)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC # Standing up our final Gold table.

-- COMMAND ----------

-- DBTITLE 1,Create our final gold table
CREATE LIVE TABLE customer_churn_gold_dlt
COMMENT "Final user table with all information for Analysis & ML."
AS 
  WITH 
    churn_orders_stats AS (SELECT user_id, count(*) as order_count, sum(amount) as total_amount, sum(item_count) as total_item, max(creation_date) as last_transaction
      FROM live.churn_orders_silver_dlt GROUP BY user_id),  
    churn_app_events_stats as (
      SELECT first(platform) as platform, user_id, count(*) as event_count, count(distinct session_id) as session_count, max(to_timestamp(date, "MM-dd-yyyy HH:mm:ss")) as last_event
        FROM live.churn_app_events_silver_dlt GROUP BY user_id)

  SELECT *, 
         datediff(now(), creation_date) as days_since_creation,
         datediff(now(), last_activity_date) as days_since_last_activity,
         datediff(now(), last_event) as days_last_event
       FROM live.churn_users_silver_dlt
         INNER JOIN churn_orders_stats using (user_id)
         INNER JOIN churn_app_events_stats using (user_id)