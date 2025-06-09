-- Databricks notebook source
select driver_name,
count(1) as total_races,
SUM(CASE WHEN position = 1 THEN 1 ELSE 0 END) AS wins,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
 from f1_presentation_kg.calculated_race_results
 group by driver_name
 having count(1) >= 50
 order by wins desc

-- COMMAND ----------



-- COMMAND ----------

select driver_name,
count(1) as total_races,
count(position) as wins,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
 from f1_presentation_kg.calculated_race_results
 group by driver_name
 having count(1) >= 50