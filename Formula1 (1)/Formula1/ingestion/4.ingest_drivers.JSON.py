# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest drivers.JSON

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Step 1 - read json file using spark dataframe reader API 

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2012-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType

# COMMAND ----------

name_schema = StructType(fields = [
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields = [
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - rename columns and add new colummns
# MAGIC 1. driverId = driver_id 
# MAGIC 2. driverRef = driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concentration of forename and surename 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns = drivers_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("driverRef", "driver_ref")\
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
.withColumn("ingestion_date", current_timestamp())\
     .withColumn("data_source", lit(v_data_source))\
        .withColumn("file date", lit(v_file_date))
display(drivers_with_columns)


# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 3 - drop unwanted columns
# MAGIC 1. name.forname 
# MAGIC 2. name.surname 
# MAGIC 3. url 

# COMMAND ----------

drivers_final_df = drivers_with_columns.drop(col("url"))
display(drivers_final_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4- write to output processed container in parguet format 

# COMMAND ----------

drivers_final_df = drivers_final_df.withColumnRenamed("file date", "file_date")
drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed_kg.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed_kg.drivers