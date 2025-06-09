# Databricks notebook source
# MAGIC %md 
# MAGIC #### Ingest LapTimes folder
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 1 - read file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                      ])

# COMMAND ----------

lap_times_df = spark.read\
.schema(lap_times_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 2 - Rename columns and add new columns 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_df = lap_times_df.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumn("ingestion date", current_timestamp())



# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 3 - Write to output to processed container in parquet format 

# COMMAND ----------

# Rename the column to remove the space
final_df = final_df.withColumnRenamed("ingestion date", "ingestion_date")

# Update the merge condition to use existing columns
merge_condition = "tgt.race_id = src.race_id and tgt.driver_id = src.driver_id"
merge_delta_data(final_df, 'f1_processed_kg', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id, count(1) 
# MAGIC from f1_processed_kg.lap_times
# MAGIC group by race_id
# MAGIC order by race_id desc;