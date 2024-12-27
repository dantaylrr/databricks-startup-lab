# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC # Databricks Start-up Lab (January 2025).
# MAGIC
# MAGIC In this short lab, we're going to be ingesting data from pre-loaded Unity Catalog volumes, performing some basic data cleansing & aggregations on the data across our medallion architecture & finally, standing up a feature table so that our Data Scientists can train a machine learning model for future inference.
# MAGIC
# MAGIC In the real world, data flows into our Data Lake at a near continuous rate, the need for real-time (often referred to as streaming) processing is ever-important for the business to make informed decisions that will impact revenue or customer satisfaction.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # General Lab Instructions.
# MAGIC
# MAGIC The following are some general pointers during the lab:
# MAGIC
# MAGIC * Due to the varying data personas taking part, we'll try to touch on all aspects of the platform. This way, everyone in the room should get some exposure to something they haven't used before.
# MAGIC * Make sure to use the [scratch]($../scratch/exploration_sql) directory for all exploration, analysis & development.
# MAGIC   * We'll put together a production-ready notebook once we have each component of our medallion architecture ready.
# MAGIC * None of the operations or transformations that we'll be doing are complex, **this isn't a bootcamp on how to be the "best" SQL / Python developer & learn all the syntax + concepts, we want you to feel comfortable in the platform & applying what we've learnt today moving forward.**
# MAGIC   * If you are well versed in SQL & manage to breeze through the lab, have a go at the exact same exercise but using only Python.
# MAGIC   * Likewise if you're most comfortable in Python, give it a try in SQL.
# MAGIC * Work through each of the exercises & don't be afraid to ask for help from any of the Solution Architects around the room.
# MAGIC   * The same applies if you have a question about anything we'll be doing today.
# MAGIC * Feel free to use any available resources to help you answer the questions, syntax issues are likely to be your biggest worry, so try one of the following:
# MAGIC   * Databricks documentation.
# MAGIC   * The built-in AI assistant.
# MAGIC   * Ask one of the Solution Architects around the room.
# MAGIC * **If you manage to get through the lab quickly, feel free to apply some of what we've done already on your own datasets! We'll also open up the floor to additional questions if there's time!**

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Data Engineering on Databricks.
# MAGIC
# MAGIC <link href="https://fonts.googleapis.com/css?family=DM Sans" rel="stylesheet"/>
# MAGIC <div style="width:300px; text-align: center; float: right; margin: 30px 60px 10px 10px;  font-family: 'DM Sans'">
# MAGIC   <div style="height: 250px; width: 300px;  display: table-cell; vertical-align: middle; border-radius: 50%; border: 25px solid #fcba33ff;">
# MAGIC     <div style="font-size: 70px;  color: #70c4ab; font-weight: bold">
# MAGIC       73%
# MAGIC     </div>
# MAGIC     <div style="color: #1b5162;padding: 0px 30px 0px 30px;">of enterprise data goes unused for analytics and decision making</div>
# MAGIC   </div>
# MAGIC   <div style="color: #bfbfbf; padding-top: 5px">Source: Forrester</div>
# MAGIC </div>
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC ## <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/de.png" style="float:left; margin: -35px 0px 0px 0px" width="80px"> Dan, as Data engineer, spends immense time….
# MAGIC
# MAGIC
# MAGIC * Hand-coding data ingestion & transformations and dealing with technical challenges:<br>
# MAGIC   *Supporting streaming and batch, handling concurrent operations, small files issues, GDPR requirements, complex DAG dependencies...*<br><br>
# MAGIC * Building custom frameworks to enforce quality and tests<br><br>
# MAGIC * Building and maintaining scalable infrastructure, with observability and monitoring<br><br>
# MAGIC * Managing incompatible governance models from different systems
# MAGIC <br style="clear: both">
# MAGIC
# MAGIC This results in **operational complexity** and overhead, requiring expert profile and ultimately **putting data projects at risk**.
# MAGIC
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection or disable tracker during installation. View README for more details.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=lakehouse&org_id=1444828305810485&notebook=%2F01-Data-ingestion%2F01.1-DLT-churn-SQL&demo_name=lakehouse-retail-c360&event=VIEW&path=%2F_dbdemos%2Flakehouse%2Flakehouse-retail-c360%2F01-Data-ingestion%2F01.1-DLT-churn-SQL&version=1">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Step 1 - Exploration of Raw.
# MAGIC
# MAGIC **1.1).** Start by exploring raw, load **all** of our data from **all** of our sources into our notebook using SQL.
# MAGIC
# MAGIC **1.2).** Explore the statistical profiles of all of our datasets, use Databricks' built-in statistical profiler for this step.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load data In-memory from our source (Orders)
# Your answer goes here

# COMMAND ----------

# DBTITLE 1,Load data In-memory from our source (Events)
# Your answer goes here

# COMMAND ----------

# DBTITLE 1,Load data In-memory from our source (Users)
# Your answer goes here

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **1.3).** Thinking as a downstream consumer of this data, what issues can you foresee with what we currently have in raw?
# MAGIC
# MAGIC **Make note of these as they'll come in handy later.**

# COMMAND ----------

# DBTITLE 1,Keep track of data quality issues
"""

Use this block to make notes on question above:

"""

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Step 2 - Standing up Bronze.
# MAGIC
# MAGIC **2.1).** Can you now stand this data up (as a **one off batch operation**) in the same catalog & schema where your data is stored?
# MAGIC

# COMMAND ----------

# DBTITLE 1,Stand up our raw data in Bronze (Orders)
# Your answer goes here

# COMMAND ----------

# DBTITLE 1,Stand up our raw data in Bronze (Events)
# Your answer goes here

# COMMAND ----------

# DBTITLE 1,Stand up our raw data in Bronze (Users)
# Your answer goes here

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC _We can quite easily stand our tables up as-is for our Bronze layer, but as we've seen from the previous exercise, there's definitely some dodgy data landing in our hyperscalar storage location._
# MAGIC
# MAGIC **Although a good start, we're missing quite a lot of additional functionality still, imagine the following scenario:**
# MAGIC
# MAGIC * Data is **continuously flowing into our hyperscalar storage location from upstream processes** (maybe a front-end application or a streaming source).
# MAGIC   * Further to this, we want to **minimise processing downtime** by ignoring schema changes in the source data, but would like to keep record of any schema changes if possible.
# MAGIC * The volume of this data is **growing exponentially** as our business grows & onboards more customers.
# MAGIC * For now, our downstream analytics & stakeholders only require a daily refresh of data, they **do not need real-time data processing right now**.
# MAGIC
# MAGIC **2.2).** Again, stand-up our raw data (**run it as a one-off batch operation for the time-being**, we'll explore freshness in a little more depth once we get to orchestrating our pipeline) as bronze tables under the same catalog & schema, this time though, make sure that **all of the above requirements are met**.
# MAGIC
# MAGIC _This works nicely, we've set-up a solid foundation for our downstream processes._

# COMMAND ----------

# DBTITLE 1,Stand up our raw data in Bronze with given requirements (Orders)
# Your answer goes here

# COMMAND ----------

# DBTITLE 1,Stand up our raw data in Bronze with given requirements (Events)
# Your answer goes here

# COMMAND ----------

# DBTITLE 1,Stand up our raw data in Bronze with given requirements (Users)
# Your answer goes here

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Step 3 - Standing up Silver.
# MAGIC
# MAGIC _We need to do some cleansing of this data before we can stand it up properly for downstream operations that are going to power business decisions._
# MAGIC
# MAGIC An additional requirement is that any asset that we produce from here on **must be stripped of any customer PII** to ensure that we meet compliance standards.
# MAGIC
# MAGIC _There are multiple ways to go about this, explore the below as options:_
# MAGIC
# MAGIC * [Row & column level masking based on Unity Catalog groups](https://docs.databricks.com/en/tables/row-and-column-filters.html).
# MAGIC * [Traditional PII masking](https://docs.databricks.com/en/sql/language-manual/functions/sha1.html).
# MAGIC
# MAGIC **3.1).** Now, stand this data up (as a **one off batch operation**) in the same catalog and schema whilst filtering for the relevant conditions you've identified from exercise **1.3).**, as well as enforcing PII masking of the following columns in the `users` table:
# MAGIC
# MAGIC * `email`
# MAGIC * `firstname`
# MAGIC * `lastname`
# MAGIC * `address`

# COMMAND ----------

# DBTITLE 1,Stand up our raw data in Silver (Orders)
# Your answer goes here

# COMMAND ----------

# DBTITLE 1,Stand up our data in Silver (Events)
# Your answer goes here

# COMMAND ----------

# DBTITLE 1,Stand up our data in Silver (Users)
# Your answer goes here

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Step 4 - Standing up Gold.
# MAGIC
# MAGIC Our gold layer should be where we perform more complex aggregations & joins. Let's envision that our customer retention team are interested in a historic view of customer churn for reporting purposes.
# MAGIC
# MAGIC They want an aggregated view of all of our silver tables. After doing some requirements gathering, we've discovered that the team is interested in the following:

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ```bash
# MAGIC customer_churn_gold
# MAGIC ├── user_id (string)
# MAGIC ├── email (string)
# MAGIC ├── creation_date (timestamp)
# MAGIC ├── last_activity_date (timestamp)
# MAGIC ├── firstname (string)
# MAGIC ├── lastname (string)
# MAGIC ├── address (string)
# MAGIC ├── canal (string)
# MAGIC ├── country (string)
# MAGIC ├── gender (int)
# MAGIC ├── age_group (int)
# MAGIC ├── churn (int)
# MAGIC ├── order_count (bigint)
# MAGIC ├── total_amount (bigint)
# MAGIC ├── total_item (bigint)
# MAGIC ├── last_transaction (timestamp)
# MAGIC ├── platform (string)
# MAGIC ├── event_count (bigint)
# MAGIC ├── session_count (bigint)
# MAGIC └── last_event (timestamp)
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC **4.1).** Stand up our gold table for reporting. Make sure that it conforms to the above schema.

# COMMAND ----------

# DBTITLE 1,Stand up our Gold table (customer_churn_gold)
# Your answer goes here

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Step 5 - Production (Jobs).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Jobs vs DLTs.
# MAGIC
# MAGIC We now have a very solid ETL pipeline, however, everything we've done so far has been ad-hoc / one-off operations in a single notebook. In the real-world, we'd need to productionise this notebook & run it on a schedule.
# MAGIC
# MAGIC There are a multitude of ways you can do this on Databricks, most notibly through Databricks jobs (sometimes referred to to as workflows) or Delta Live Tables (DLTs). Let's explore both options now.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Databricks Jobs.
# MAGIC
# MAGIC [Databricks jobs](https://docs.databricks.com/en/jobs/index.html#what-are-databricks-jobs) are the most common way to orchestrate different operations & workloads on Databricks.
# MAGIC
# MAGIC They are also the most cost-efficient & flexible way to orchestrate workloads on Databricks but have the trade-off of being **a lot more hands-on & requiring a lot more overall maintenance**.
# MAGIC
# MAGIC Let's start by productionising the workflow we've just put together, making sure that it conforms to the requirements set-out above.
# MAGIC
# MAGIC **5.1).** In your own workspace, create a Databricks job that periodically orchestrates the steps we've outlined above.
# MAGIC * In a typical production environment, our entire workflow would be split-up into different [job tasks](https://docs.databricks.com/en/jobs/configure-task.html). A common approach here it to have each operation as a different notebook, SQL or Python file & define dependancies in a DAG-like manner. This approach has been implemented under the [`answers/`](./answers/) directory, feel free to use these notebooks instead of this one. \
# MAGIC \
# MAGIC _Why might it be important to avoid scheduling a single notebook containing all of our operations compared to multiple?_
# MAGIC
# MAGIC * **Use serverless** as your compute resource & schedule your job to run daily at a suitable time of your choosing. \
# MAGIC \
# MAGIC _Notice the configurable job [trigger types](https://docs.databricks.com/en/jobs/index.html#trigger-types), we could've set this to `File Arrival` rather than using autoloader, however, this is unoptimal as we lose a lot of the built-in functionality of autoloader (schema rescue, evolution, etc.) as well as increased cloud costs due to minutely API `LIST` operations._
# MAGIC
# MAGIC * **Have a play around with some of the built-in orchestration options**:
# MAGIC   * Conditional tasks.
# MAGIC   * For each (looped) tasks.
# MAGIC   * If/else conditional tasks.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Streaming vs Batch.
# MAGIC
# MAGIC We've now had an additional requirement come in from the customer retention team, they now require the freshest, most accurate data to make **critical business decisions that will impact revenue, customer satisfaction & the overall business' success**.
# MAGIC
# MAGIC We now need to turn this batch pipeline that runs daily into an operational streaming pipeline.
# MAGIC
# MAGIC Streaming pipelines incur lots of additional operational complexity, for example, consider the following:
# MAGIC
# MAGIC * Incremental processing as opposed to full-scans (event-based processing).
# MAGIC * Stateful vs stateless pipelines & operations.
# MAGIC * Costs associated with 24/7 streaming resources.
# MAGIC * An understanding of window functions & micro-batch processing using methods like `for_each`.
# MAGIC * etc...
# MAGIC
# MAGIC **You can quickly start to see how, without experience & expertise in this area, the barrier to entry can be relatively steep. This takes away precious hours & resources that could be spent elsewhere.**
# MAGIC
# MAGIC ### Optional.
# MAGIC
# MAGIC **5.2).** Implement a new notebook & workflow that conforms to this contraint using:
# MAGIC
# MAGIC * Spark structured-streaming.
# MAGIC
# MAGIC _Don't worry about completing this question unless you have time at the end, we will implement the above in Delta Live Tables first._

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Step 6 - Production (DLTs).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Delta Live Tables (DLTs).
# MAGIC
# MAGIC [Delta Live Tables](https://docs.databricks.com/en/delta-live-tables/index.html) (often referred to as DLTs) are a declarative framework for building both batch & streaming workloads using either SQL or Python.
# MAGIC
# MAGIC Delta live tables have a bunch of handy features that come out-of-the-box, including:
# MAGIC
# MAGIC * Built-in data quality constraints.
# MAGIC * Ability to define streaming tables in ANSI SQL syntax.
# MAGIC * Automatic dependancy management.
# MAGIC * Built-in pipeline observability.
# MAGIC
# MAGIC **Delta Live Tables offer an extremely user-friendly way to get up-and-running in a streaming environment for users who don't have experience or expertise in Spark structured streaming & want a more managed streaming experience.**
# MAGIC
# MAGIC **6.1).** Use Databricks' [available documentation](https://docs.databricks.com/en/delta-live-tables/index.html) to turn our previous job into a Delta Live pipeline.
# MAGIC
# MAGIC * Ensure that all of our tables are either materialized views or streaming tables.
# MAGIC * Enforce any relevant [data quality constraints](https://docs.databricks.com/en/delta-live-tables/expectations.html) you deem necessary, as we did in section **3.1).** above.
# MAGIC * Use serverless as our compute resource.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Additional Exercises - DevEx.
# MAGIC
# MAGIC As a stretch exercise, why don't you try installing & configuring [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html) & deploying your new Workflow & DLT from the CLI?
# MAGIC
# MAGIC * Install & configure the Databricks CLI (if you haven't already).
# MAGIC * Don't worry about version control or CI/CD, can you bootstrap a local directory using some of the built-in CLI commands?
# MAGIC * You might have to make a few changes, but can you deploy both your workflow & DLT to your dev. workspace from your local terminal?
# MAGIC
# MAGIC _Hints: Start by reading through the [documentation](https://docs.databricks.com/en/dev-tools/bundles/index.html) & having a play around with some of the configuration. If you're curious how DABs works behind the scenes before you deploy anything, let a Solution Architect know!_
# MAGIC
# MAGIC **Don't worry if this isn't immediately doable or clear, a Solution Architect around the room might be able to help you provide you have the CLI installed - give it a try!**

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Additional Exercises - Data Science.
# MAGIC
# MAGIC As a stretch exercise, let's use our newly created gold table to train a machine-learning model that will help predict future customer churn.
# MAGIC
# MAGIC Ideally, we would create a feature store table here so that our model can lookup relevant user features at inference run-time (as opposed to the source system having knowledge of what these features are, adding inference latency).
# MAGIC
# MAGIC This **isn't necessarily required** however, we can just train a model on our historic data.
# MAGIC
# MAGIC Try the following:
# MAGIC
# MAGIC **Note - you will need to create (if one doesn't already exist) a cluster with the ML runtime in order to try the below exercises.**
# MAGIC
# MAGIC * (**Optional**) Create a feature store table in our catalog & schema - remove any unwanted columns from our gold table:
# MAGIC   * Use the [`FeatureStoreClient`](https://api-docs.databricks.com/python/feature-store/latest/feature_store.client.html) API.
# MAGIC   * Use Databricks [online tables](https://docs.databricks.com/en/machine-learning/feature-store/online-tables.html).
# MAGIC * Use [AutoML](https://docs.databricks.com/en/machine-learning/automl/index.html) to train a classification model that will predict customer churn based on our data in our gold table (or, if you created a feature store table earlier, use that instead).
# MAGIC * Register the model to your catalog.
# MAGIC * Apply that model to a specific dataset, compare it's performance to the observed results.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC # Appendix.
# MAGIC
# MAGIC Sample answers to the above questions can be found under the `answers/` directory.