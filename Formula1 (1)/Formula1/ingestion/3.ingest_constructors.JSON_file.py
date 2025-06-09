# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest Constructors.json file

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 1 - read the JSON file using the spark dataframe reader

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2012-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_dif = spark.read\
    .schema(constructor_schema)\
    .json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Drop unwanted columns from the dataframe
# MAGIC

# COMMAND ----------

constructors_dropped_dif = constructor_dif.drop("url")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 3 - Rename columns and add ingestion date 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
constructor_final_df = constructors_dropped_dif.withColumnRenamed("constructorId", "constructor_id")\
    .withColumnRenamed("constructorRef", "constructor_ref")\
        .withColumn("ingestion_date", current_timestamp())\
            .withColumn("data_source", lit(v_data_source))\
        .withColumn("file date", lit(v_file_date))



# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write back into parquet file

# COMMAND ----------

constructor_final_df = constructor_final_df.withColumnRenamed("file date", "file_date")
constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed_kg.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed_kg.constructors

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/internship/Kaelin/processed/constructors"))

# COMMAND ----------

