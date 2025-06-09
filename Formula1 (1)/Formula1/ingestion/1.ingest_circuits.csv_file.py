# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC step 1 - read the csv file using the dataframe reader

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

# MAGIC %md
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuit_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### select only required columns

# COMMAND ----------

circuits_df = spark.read \
.schema(circuit_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")


# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)
display(circuits_selected_df)

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])
display(circuits_selected_df)

# COMMAND ----------

from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))
display(circuits_selected_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %md 
# MAGIC ## step 3 - Rename the columns as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id").withColumnRenamed("circuitRef", "circuit_ref")\
.withColumnRenamed("lat", "latitude").withColumnRenamed("lng", "longitude").withColumnRenamed("alt", "altitude")\
    .withColumn("data_source", lit(v_data_source))\
        .withColumn("file date", lit(v_file_date))
display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 4 - add ingestion date to the dataframe

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)
display(circuits_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Step 5 -Write to datalake as parquet

# COMMAND ----------

circuits_final_df = circuits_final_df.withColumnRenamed("file date", "file_date")
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed_kg.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE f1_processed_kg

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed_kg.circuits

# COMMAND ----------

dbutils.notebook.exit("Success")