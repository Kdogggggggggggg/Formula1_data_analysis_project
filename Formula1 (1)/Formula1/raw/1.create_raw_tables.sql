-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw

-- COMMAND ----------

drop table if exists f1_raw.circuits;

create table f1_raw.circuits(
  circuitId string,
  circuitRef string,
  name string,
  location string,
  country string,
  lat double,
  lng double,
  alt int,
  url string  
)
using csv 
options (path "dbfs:/internship/Kaelin/raw/circuits.csv")

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

drop table if exists f1_raw.races;

create table f1_raw.races(
  raceId int,
  year integer,
  round int,
  circuitId int,
  name string,
  date date,
  time string,
  url string 
)
using csv 
options (path "dbfs:/internship/Kaelin/raw/races.csv")

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

drop table if exists f1_raw.constructors;

create table f1_raw.constructors(
  constructorId int,
  constructorRef string,
  name string,
  nationality string,
  url string
)
using json
location "dbfs:/internship/Kaelin/raw/constructors.json"

-- COMMAND ----------

select * from f1_raw.constructors

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(
  driverId int,
  driverRef string,
  number int,
  code string,
  name struct<forename:string, surname:string>,
  dob date,
  nationality string,
  url String
)
using json 
options (path "dbfs:/internship/Kaelin/raw/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

drop table if exists f1_raw.results;

create table if not exists f1_raw.results (
  resultId int,
  raceId int,
  driverId int,
  constructorId int,
  number int,
  grid int,
  position int,
  positionText string,
  positionOrder int,
  points float,
  laps int,
  time string,
  milliseconds int,
  fastestLap int,
  rank int,
  fastestLapTime string,
  fastestLapSpeed float,
  statusId int
)
using json
options (path "dbfs:/internship/Kaelin/raw/results.json");

select * from f1_raw.results;

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;

create table if not exists f1_raw.pit_stops (
  raceId int,
  driverId int,
  stop String,
  lap int,
  time String,
  duration string,
  milliseconds int
)
using json 
options (path "dbfs:/internship/Kaelin/raw/pit_stops.json", multiLine true)


-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### reading data from a list of files 

-- COMMAND ----------

drop table if exists f1_raw.lap_times;

create table if not exists f1_raw.lap_times (
  raceId int,
  driverId int,
  lap int,
  position int,
  time string,
  milliseconds int
)
using csv 
options (path "dbfs:/internship/Kaelin/rawlap_times")

-- COMMAND ----------

select count(1) from f1_raw.lap_times

-- COMMAND ----------

drop table if exists f1_raw.qualifying;

create table if not exists f1_raw.qualifying (
  qualifyId int,
  raceId int,
  driverId int,
  constructorId int,
  number int,
  position int,
  q1 string,
  q2 string,
  q3 string
)
using json 
options (path "dbfs:/internship/Kaelin/rawqualifying", multiLine true)

-- COMMAND ----------

select count(1) from f1_raw.qualifying

-- COMMAND ----------

