-- Databricks notebook source
create database if not exists f1_processed_kg
location "dbfs:/internship/Kaelin/processed" 

-- COMMAND ----------

create database if not exists f1_presentation_kg
location "dbfs:/internship/Kaelin/presentation" 

-- COMMAND ----------

desc database f1_processed_kg

-- COMMAND ----------

