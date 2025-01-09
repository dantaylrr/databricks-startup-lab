# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Start-up Lab (January 2025) - Part 2
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## BI & Datawarehousing with Databricks SQL
# MAGIC ## 
# MAGIC <img style="float: right; margin-top: 10px" width="500px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/retail/lakehouse-churn/lakehouse-retail-c360-churn-3.png" />
# MAGIC
# MAGIC Our datasets are now properly ingested, secured, with a high quality and easily discoverable within our organization.
# MAGIC
# MAGIC Let's explore how Databricks SQL support your Data Analyst team with interactive BI and start analyzing our customer Churn.
# MAGIC
# MAGIC To start with Databricks SQL, open the SQL view on the top left menu.
# MAGIC
# MAGIC You'll be able to:
# MAGIC
# MAGIC - Create a SQL Warehouse to run your queries
# MAGIC - Use DBSQL to build your own dashboards
# MAGIC - Plug any BI tools (Tableau/PowerBI/..) to run your analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Creating SQL Queries 
# MAGIC
# MAGIC
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-dbsql-query.png" />
# MAGIC
# MAGIC Our users can now start running SQL queries using the SQL editor and add new visualizations.
# MAGIC
# MAGIC The data engineering team has asked you as a data analyst for some key insights that will need to shared with different stakeholers in the organizations, we are looking for the following insights:
# MAGIC - Churned vs unchurned customers by the operating system they are using (platform)
# MAGIC - Total churned customers by Gender
# MAGIC - Monthly recurring revenue over time between
# MAGIC - Total amount of customers who have churned 
# MAGIC
# MAGIC Databricks SQL queries use a special type of compute called [SQL compute](https://docs.databricks.com/en/compute/sql-warehouse/index.html) which is optimized for running highly concurrent BI queries. Feel free to check out [this doc](https://docs.databricks.com/en/compute/sql-warehouse/index.html#create-a-sql-warehouse) for steps to create one if you don't already have one. 
# MAGIC
# MAGIC
# MAGIC **Tip 1**:
# MAGIC You can use the AI Assistant to get some help in generating those queries. 
# MAGIC
# MAGIC **Tip 2**:
# MAGIC You can use the [SQL language reference doc](https://docs.databricks.com/en/sql/language-manual/index.html) if you need some helo generating those queries. 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Step 2: Let's start Dashboarding!
# MAGIC
# MAGIC <img style="float: right; margin-left: 10px" width="600px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/retail/lakehouse-churn/lakehouse-retail-c360-dashboard-churn-prediction.png?raw=true" />
# MAGIC
# MAGIC The next step is now to assemble our queries and their visualization in a comprehensive SQL dashboard that our business will be able to track. This step is divided into four sub steps:
# MAGIC
# MAGIC
# MAGIC **Step 2.1** Create a report that displays 4 different visualizations from the datasets that were created in the previous step using SQL queries. 
# MAGIC  
# MAGIC **Step 2.2** Add a date filter that allows us to view those visualizations by a specific timeframe. 
# MAGIC
# MAGIC **Step 2.3** Publish the dashboard and make it viewable by others in the orranization. 
# MAGIC
# MAGIC **Step 2.4** Ensure that this Dashboard refreshes on a bi-weekly basis. 
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Going beyond Dashboards with Genie
# MAGIC
# MAGIC <img style="float: left; margin-top: 10px" width="600px" src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/retail/lakehouse-churn/lakehouse-retail-churn-ai-bi.png?raw=true">
# MAGIC  
# MAGIC
# MAGIC Dashboards are a great way for us to analyze and get insights of our data to answer specifc reccuring questions about the business, but what if we need more? What is our users have questions beyond what a dashboard can offer us? Instead of continuing to add one-off visualizations to a report which may or may not be helpful to others, we could instead use Genie to ask natural language questions and enable other users to do the same. This is what we will do in this step.
# MAGIC
# MAGIC Some stakeholders within the organization have asked for some additional insights to the Dahsboard you've built for them, here is when we'll create a genie space that will allow them to ask any adhoc questions they have of their data and enable a more self-analytics solutions for them. The Genie room will need to be able to answer the following questions: 
# MAGIC
# MAGIC **Step 3.1**: Create a new Genie room, this is what we'll use to ask questions of our data, you can follow the steps in [this doc](https://docs.databricks.com/en/genie/set-up.html#create-a-new-genie-space).
# MAGIC
# MAGIC **Step 3.2** When creating any genie space we have to set it up with the datasets that we need to ask the data of. As a best practice we need to ensure that all datasets are well documented. To do so, we'll head over to the Catalog explorer, select the datasets and ensure the table and columns have comment descriptions, see here for [steps](https://docs.databricks.com/en/comments/ai-comments.html#add-ai-generated-comments). Once we have this, we can now start asking questions of our Genie space.
# MAGIC
# MAGIC **Step 3.3** Start by generating responses to the following queries:
# MAGIC - How many customers have churned over time? (compare that to the answer of the SQL query in the previous step)
# MAGIC - What is the average monthly payment by platform?
# MAGIC - What is the total Revenue in 2015?
# MAGIC - What is the churn rate for each country represented in the user base?
# MAGIC - What was the daily revenue from June 1 to June 9 2023?
# MAGIC
# MAGIC For each questions you ask, make sure to view the underlying SQL that is generated. You will notice in some questions Genie might not get the answer right because of some missing context, here it is up to use to provide it the right business rules that it needs to use to answer questions correctly. 
# MAGIC
# MAGIC Here it is up to us to fill in any gaps in knowledge by using [instructions](https://docs.databricks.com/en/genie/set-up.html#general-instructions) or Sample SQL Queries. 
# MAGIC
# MAGIC **Step 3.4** For each question we need to ensure the following:
# MAGIC - When calculating churn rates we need to ensure we eliminate null values.
# MAGIC - When asked about a fiscal quarter, we need Genie to know that we are in Q4 and that the Fiscal year starts in Feb. 
# MAGIC - When asked about the top churned users, ensure answers are sorted descendingly. 
# MAGIC
# MAGIC
# MAGIC
