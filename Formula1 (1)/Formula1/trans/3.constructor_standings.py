# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
.filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, "race_year")

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

constructor_df = race_results_df\
    .groupBy("race_year", "team")\
    .agg(
        sum("points").alias("total_points"),
        count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_df.withColumn("rank", rank().over(constructor_rank_spec))
display(final_df)

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation_kg.constructor_standings")
# overwrite_partition(final_df, "f1_presentation_kg", "constructor_standings", "race_year")

merge_condition = "tgt.team = src.team and tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation_kg', 'team_results', presentation_folder_path, merge_condition, 'race_year')