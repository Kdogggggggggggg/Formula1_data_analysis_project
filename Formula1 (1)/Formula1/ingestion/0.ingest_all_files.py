# Databricks notebook source
v_result = dbutils.notebook.run(
    "/Workspace/Users/kaelin.good@sdktek.com/Formula1/ingestion/1.ingest_circuits.csv_file",
    0,
    {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"}
)

# COMMAND ----------



# COMMAND ----------

v_result = dbutils.notebook.run("2.Ingest_races.csv_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"}
)

# COMMAND ----------

v_result = dbutils.notebook.run("/Workspace/Users/kaelin.good@sdktek.com/Formula1/ingestion/3.ingest_constructors.JSON_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"}
)

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_drivers.JSON", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"}
)

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingets_results.JSON", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"}
)

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_pitstops.JSON_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"}
)

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_LapTimesSplit.csv_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"}
)

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest_QualifyingSplit.JSON_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"}
)

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE f1_processed_kg.results;
# MAGIC
# MAGIC select * from f1_processed_kg.results