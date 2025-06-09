-- Databricks notebook source
create database if not exists demo;

-- COMMAND ----------

show databases


-- COMMAND ----------

describe database demo

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables

-- COMMAND ----------

use demo

-- COMMAND ----------

select current_database()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## creating managed tables 

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").mode("overwrite").saveAsTable("demo.race_results_python")

-- COMMAND ----------

use demo;
show tables

-- COMMAND ----------

desc extended race_results_python

-- COMMAND ----------

select * 
from demo.race_results_python
where race_year = 2020

-- COMMAND ----------

create table race_results_sql
as
select * 
from demo.race_results_python
where race_year = 2020

-- COMMAND ----------

select current_database()

-- COMMAND ----------

desc extended demo.race_results_sql

-- COMMAND ----------

drop table demo.race_results_sql

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### External tables 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py", mode='overwrite')

-- COMMAND ----------

desc extended demo.race_results_ext_py

-- COMMAND ----------

create table IF NOT EXISTS demo.race_results_ext_sql
(
  race_year int, 
  race_name string, 
  race_date timestamp, 
  circuit_location string, 
  driver_name string,
  driver_number int,
  driver_nationality string,
  team string,
  grid int,
  fastest_lap int,
  race_time string,
  points float,
  position int,
  created_date timestamp
)
using parquet 
location "dbfs:/internship/Kaelin/presentation/race_results_ext_sql"

-- COMMAND ----------

insert into demo.race_results_ext_sql
select race_year, race_name, race_date, circuit_location, driver_name, null as driver_number, driver_nationality, team, grid, fastest_lap, race_time, points, position, created_date
from demo.race_results_ext_py
where race_year = 2020

-- COMMAND ----------

SHOW TABLES IN demo



-- COMMAND ----------

drop table demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES IN demo


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Views on tables 
-- MAGIC 1. create temp view
-- MAGIC
-- MAGIC 2. create global temp view
-- MAGIC
-- MAGIC 3. create permanent view 
-- MAGIC

-- COMMAND ----------

create or replace temp view v_race_results 
as
select * 
from demo.race_results_python
where race_year = 2020

-- COMMAND ----------

select * from v_race_results

-- COMMAND ----------

create or replace global temp view gv_race_results 
as
select * 
from demo.race_results_python
where race_year = 2012

-- COMMAND ----------

select * from global_temp.gv_race_results

-- COMMAND ----------

create or replace view demo.pv_race_results 
as
select * 
from demo.race_results_python
where race_year = 2000

-- COMMAND ----------

show tables in demo


-- COMMAND ----------

select * from demo.pv_race_results

-- COMMAND ----------

