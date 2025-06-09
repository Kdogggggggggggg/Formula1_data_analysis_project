-- Databricks notebook source
select team_name, 
count(*) as total_races,
sum(calculated_points) as total_points,
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year between 2010 and 2020
group by team_name
having count(*)>= 100
order by avg_points desc