-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Drop all tables 

-- COMMAND ----------

drop database if exists f1_processed_kg cascade

-- COMMAND ----------

create database if not exists f1_processed_kg
location "dbfs:/internship/Kaelin/processed"

-- COMMAND ----------

drop database if exists f1_presentation_kg cascade;

create database if not exists f1_presentation_kg
location "dbfs:/internship/Kaelin/presentation"

-- COMMAND ----------



-- COMMAND ----------

-- %python
-- # Folder containing the files to delete
-- folder = "dbfs:/internship/Kaelin/raw"

-- # List all items in the folder
-- files = dbutils.fs.ls(folder)

-- # Loop through and delete each item
-- for f in files:
--     dbutils.fs.rm(f.path, recurse=True)
--     print(f"Deleted: {f.path}")



-- COMMAND ----------

