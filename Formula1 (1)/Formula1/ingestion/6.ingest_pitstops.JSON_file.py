# Databricks notebook source
# MAGIC %md 
# MAGIC #### Ingest pit_stops.json file

# COMMAND ----------



# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------



# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 1 - read JSON file using the spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                      ])

# COMMAND ----------

pit_stops_df = spark.read\
.schema(pit_stops_schema)\
    .option("multiLine", True)\
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 2 - Rename columns and add new columns 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_df = pit_stops_df.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumn("ingestion date", current_timestamp())
display(final_df)


# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 3 - Write to output to processed container in parquet format 

# COMMAND ----------

# re_arrange_partition_column(final_df, "race_id")
# overwrite_partition(final_df, "f1_processed_kg", "pit_stops", "race_id")

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed_kg.pit_stops")
# display(spark.read.parquet("dbfs:/internship/Kaelin/processed/pit_stops"))

# COMMAND ----------

# Rename the column to remove the space
final_df = final_df.withColumnRenamed("ingestion date", "ingestion_date")

# Update the merge condition to use existing columns
merge_condition = "tgt.race_id = src.race_id and tgt.driver_id = src.driver_id"
merge_delta_data(final_df, 'f1_processed_kg', 'pit_stop', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id, count(1) 
# MAGIC from f1_processed_kg.pit_stop
# MAGIC group by race_id
# MAGIC order by race_id desc;