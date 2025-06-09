# Databricks notebook source
# MAGIC %sql
# MAGIC use f1_processed_kg

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""
          
          create table if not exists f1_presentation_kg.calculated_race_results
          (
            race_year int,
            team_name string,
            driver_id int,
            driver_name string,
            race_id int,
            position int,
            points int,
            calculated_points int,
            created_date timestamp,
            updated_date timestamp
          )
          using delta
""")

# COMMAND ----------

spark.sql(f"""
                    create or replace temp view race_result_updated
                    as 
                    select races.race_year, 
                    constructors.name as team_name,
                    drivers.driver_id,
                    drivers.name as driver_name,
                    races.race_id,
                    results.position,
                    results.points,
                    11 - results.position as calculated_points
                    from f1_processed_kg.results 
                    join f1_processed_kg.drivers on (results.driver_id = drivers.driver_id)
                    join f1_processed_kg.constructors on (results.constructor_id = constructors.constructor_id)
                    join f1_processed_kg.races on (results.race_id = races.race_id)
                    where results.position <= 10
                    and results.file_date = '{v_file_date}'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_presentation_kg.calculated_race_results as tgt
# MAGIC USING race_result_updated as upd
# MAGIC ON (tgt.driver_id = upd.driver_id and tgt.race_id = upd.race_id)
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET tgt.position = upd.position,
# MAGIC            tgt.points = upd.points,
# MAGIC            tgt.calculated_points = upd.calculated_points,
# MAGIC            tgt.updated_date = current_timestamp()
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, created_date)
# MAGIC   VALUES (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, current_timestamp)

# COMMAND ----------

# %sql
# create table f1_presentation_kg.calculated_race_results 
# using parquet 
# as 
# select races.race_year, 
# constructors.name as team_name,
# drivers.name as driver_name,
# results.position,
# results.points,
# 11 - results.position as calculated_points
# from results 
# join f1_processed_kg.drivers on (results.driver_id = drivers.driver_id)
# join f1_processed_kg.constructors on (results.constructor_id = constructors.constructor_id)
# join f1_processed_kg.races on (results.race_id = races.race_id)
# where results.position <= 10


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation_kg.calculated_race_results

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(1) from race_result_updated

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from f1_presentation_kg.calculated_race_results