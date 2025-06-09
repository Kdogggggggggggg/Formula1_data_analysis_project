# Databricks notebook source
# MAGIC %run "/Workspace/Users/kaelin.good@sdktek.com/Formula1/includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
.filter("circuit_id < 70")\
.withColumnRenamed("name", "circuit_name")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner"). select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.name.alias("race_name"), races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

#left outer join 
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left"). select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.name.alias("race_name"), races_df.round)
display(race_circuits_df)

# COMMAND ----------

#right outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right"). select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.name.alias("race_name"), races_df.round)
display(race_circuits_df)

# COMMAND ----------

#full outer
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full"). select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.name.alias("race_name"), races_df.round)
display(race_circuits_df)

# COMMAND ----------

#semi joins 
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi"). select(circuits_df.circuit_name, circuits_df.location, circuits_df.country)
display(race_circuits_df)

# COMMAND ----------

# anti joins 
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti")
display(race_circuits_df)

# COMMAND ----------

#cross joins
race_circuits_df = races_df.crossJoin(circuits_df) 
display(race_circuits_df)

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")
display(drivers_df)

# COMMAND ----------

LapTime_df = spark.read.parquet(f"{processed_folder_path}/lap_times")
display(LapTime_df)

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results")
display(results_df)

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors")
display(constructors_df)

# COMMAND ----------

drivers_results = drivers_df.join(results_df, drivers_df.driver_id == results_df.driver_id, "inner")
display(drivers_results)

# COMMAND ----------

dr_constructors = drivers_results.join(constructors_df, drivers_results.constructor_id == constructors_df.constructor_id, "inner")
display(dr_constructors)

# COMMAND ----------

drcon_race = dr_constructors.join(races_df, dr_constructors.race_id == races_df.race_id, "inner")
display(drcon_race)

# COMMAND ----------

drive_race_circuit = drcon_race.join(circuits_df, drcon_race.circuit_id == circuits_df.circuit_id, "inner").select (races_df.race_year.alias("race_year"), races_df.name.alias("race_name"), drcon_race.date, circuits_df.location.alias("race_location"), drcon_race.name.alias("driver_name"), drcon_race.number.alias("driver_number"), drcon_race.nationality.alias("driver_nationality"), constructors_df.name.alias("team"), drcon_race.grid, drcon_race.fastest_lap, drcon_race.points)\
    .withColumn("created_date", current_timestamp())
display(drive_race_circuit)
