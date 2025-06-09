# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType

# COMMAND ----------

spark.read.json("dbfs:/internship/Kaelin/raw/2021-03-21/results.json").createOrReplaceTempView("results_cutover")

# COMMAND ----------

# MAGIC %sql
# MAGIC select raceId, count(1)
# MAGIC from results_cutover
# MAGIC group by raceId
# MAGIC order by raceId desc

# COMMAND ----------



# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source  = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True), 
                                      StructField("grid", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("positionText", StringType(), True),
                                      StructField("positionOrder", IntegerType(), True),
                                      StructField("points", FloatType(), True),
                                      StructField("laps", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True),
                                      StructField("fastestLap", IntegerType(), True),
                                      StructField("rank", IntegerType(), True),
                                      StructField("fastestLapTime", StringType(), True),
                                      StructField("fastestLapSpeed", FloatType(), True),
                                      StructField("statusId", StringType(), True)
                                      ])


# COMMAND ----------

results_df = spark.read\
    .schema(results_schema)\
    .json(f"{raw_folder_path}/{v_file_date}/results.json")



# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

results_selected_df = results_df.drop("statusId")


# COMMAND ----------

results_rename_df = results_selected_df.withColumnRenamed("resultId", "result_id")\
    .withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("constructorId", "constructor_id")\
    .withColumnRenamed("positionText", "position_text")\
    .withColumnRenamed("positionOrder", "position_order")\
    .withColumnRenamed("fastestLap", "fastest_lap")\
    .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
    .withColumn("ingestion_date", current_timestamp())\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date))


# COMMAND ----------

# MAGIC %md 
# MAGIC #### Method 1
# MAGIC

# COMMAND ----------

# for race_id_list in results_rename_df.select("race_id").distinct().collect():
#     if (spark.catalog.tableExists("f1_processed_kg.results")):
#      spark.sql(f"ALTER TABLE f1_processed_kg.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df = results_rename_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed_kg.results")
# display(spark.read.parquet("dbfs:/internship/Kaelin/processed/results"))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### method 2

# COMMAND ----------

# MAGIC %sql 
# MAGIC --drop table f1_processed_kg.results

# COMMAND ----------

results_final_df = results_rename_df.select("result_id", "driver_id", "constructor_id", "number", "grid", "position", "position_text", "position_order", "points", "laps", "time", "milliseconds", "fastest_lap", "rank", "fastest_lap_time", "fastest_lap_speed", "data_source", "file_date", "ingestion_date", "race_id")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### de-dupe he dataframe 

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------



# COMMAND ----------

# overwrite_partition(results_rename_df, "f1_processed_kg","results", "race_id")

# COMMAND ----------

# # Assuming results_rename_df is your DataFrame
# results_rename_df.write.format("delta").mode("overwrite").saveAsTable("f1_processed_kg.results")

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id and tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'f1_processed_kg', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

