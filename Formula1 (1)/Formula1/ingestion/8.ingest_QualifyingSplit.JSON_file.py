# Databricks notebook source
# MAGIC %md 
# MAGIC #### Ingest Qualifying.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 1 - read JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                       StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                     StructField("q3", StringType(), True),
  
                                      ])

# COMMAND ----------

qualifying_split_df = spark.read\
.schema(qualifying_schema)\
.option("multiLine", True)\
.json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 2 - Rename columns and add new columns 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_df = qualifying_split_df.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumnRenamed("qualifyId", "qualify_id")\
.withColumnRenamed("constructorId", "constructor_id")\
.withColumn("ingestion date", current_timestamp())



# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 3 - Write to output to processed container in parquet format 

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed_kg.qualifying")

# re_arrange_partition_column(final_df, "race_id")
# overwrite_partition(final_df, "f1_processed_kg", "qualifying", "race_id")

# display(spark.read.parquet("dbfs:/internship/Kaelin/processed/qualifying"))

# Rename the column to remove the space
final_df = final_df.withColumnRenamed("ingestion date", "ingestion_date")

# Update the merge condition to use existing columns
merge_condition = "tgt.race_id = src.race_id and tgt.driver_id = src.driver_id"
merge_delta_data(final_df, 'f1_processed_kg', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id, count(1) 
# MAGIC from f1_processed_kg.qualifying
# MAGIC group by race_id
# MAGIC order by race_id desc;