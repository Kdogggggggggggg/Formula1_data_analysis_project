-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Arial">
-- MAGIC Report on Dominant Formula 1 Teams
-- MAGIC </h1>"""
-- MAGIC displayHTML(html)
-- MAGIC

-- COMMAND ----------

create or replace temp view v_dom_teams
as
select race_year, team_name, 
count(*) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points,
rank() over(order by avg(calculated_points) desc) as team_rank
from f1_presentation.calculated_race_results
group by race_year, team_name


-- COMMAND ----------

select race_year, team_name,
count(1) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
 from f1_presentation_kg.calculated_race_results
 where team_name in (select team_name from v_dom_teams where team_rank <=10)
 group by race_year, team_name
order by race_year, avg_points desc