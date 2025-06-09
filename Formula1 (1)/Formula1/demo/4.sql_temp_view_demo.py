# Databricks notebook source
# MAGIC %md
# MAGIC #### Access dataframe using SQL 
# MAGIC Objectives
# MAGIC 1.Create Temp views on dataframes 
# MAGIC
# MAGIC 2.Access the view from SQL cell
# MAGIC
# MAGIC 3.Access the view from python cell

# COMMAND ----------

# MAGIC  %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

race_results_df = spark.sql("Select * from v_race_results where race_year = 2019")
display(race_results_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Global Temp View 
# MAGIC
# MAGIC 1. Create global temp view on dataframes
# MAGIC
# MAGIC 2. Access the view from SQL cell 
# MAGIC
# MAGIC 3. Access the view from Python cell 
# MAGIC
# MAGIC 4. Access the view from another notebook 

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results;

# COMMAND ----------

race_results_df = spark.sql("select * from global_temp.gv_race_results").show()

# COMMAND ----------

