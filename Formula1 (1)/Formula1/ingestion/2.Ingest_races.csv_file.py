# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - read data from csv
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2012-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/internship/Kaelin/raw"))

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                   StructField("year", IntegerType(), True),
                                   StructField("round", IntegerType(), True),
                                   StructField("circuitId", IntegerType(), True),
                                   StructField("name", StringType(), True),
                                   StructField("date", DateType(), True),
                                   StructField("time", StringType(), True),
                                   StructField("url", StringType(), True)
])


# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

races_df.printSchema()
display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select required columns

# COMMAND ----------

from pyspark.sql.functions import col

races_selected_df = races_df.select(
    col("raceId"),
    col("year"),
    col("round"),
    col("circuitId"),
    col("name"),
    col("date"),
    col("time")
)
display(races_selected_df)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, lit
races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
    .withColumnRenamed("circuitId", "circuit_id")\
    .withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))\
        .withColumn("data_source", lit(v_data_source))\
        .withColumn("file_date", lit(v_file_date))
display(races_renamed_df)




# COMMAND ----------

races_without_date_time_df = races_renamed_df.drop("time", "date")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
races_final_df = races_without_date_time_df.withColumn("ingestion_date", current_timestamp())
display(races_final_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### write back to datalake as a parquet

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed_kg.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed_kg.races