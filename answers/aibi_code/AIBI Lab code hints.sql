-- Databricks notebook source
-- Churned vs unchurned customers by the operating system they are using (platform)
select platform, churn, count(*) as event_count from churn_app_events_silver inner join churn_users_silver using (user_id) where platform is not null
group by platform, churn

-- COMMAND ----------

-- Churned customers by gender
SELECT gender, count(gender) as total_churn FROM customer_churn_gold where churn = 1 GROUP BY gender

-- COMMAND ----------

-- Monthly recurring revenue

SELECT sum(amount)/10 as MRR FROM churn_orders_silver o WHERE month(to_timestamp(creation_date, 'MM-dd-yyyy HH:mm:ss')) = 
      (select max(month(to_timestamp(creation_date, 'MM-dd-yyyy HH:mm:ss'))) from churn_orders_silver);

-- COMMAND ----------

--MRR over time

SELECT sum(amount), date_format(to_timestamp(churn_users_silver.creation_date, "MM-dd-yyyy H:mm:ss"), "yyyy-MM") m FROM churn_orders_silver o 
		INNER JOIN churn_users_silver using (user_id)
			group by m

-- COMMAND ----------


-- Total amount of customers who have churned

SELECT count(*) as past_churn FROM customer_churn_gold WHERE churn=1;
