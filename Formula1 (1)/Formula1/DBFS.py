# Databricks notebook source
display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/internship/Kaelin/raw'))

# COMMAND ----------

display(spark.read.csv('dbfs:/FileStore/circuits.csv'))

# COMMAND ----------

# # Define source and destination paths
# source_path = "dbfs:/internship/Theresa/raw/"
# destination_path = "dbfs:/internship/Kaelin/raw/"

# # 1. Delete all contents in destination path (Kaelin/raw)
# if dbutils.fs.ls(destination_path):  # Check if it exists and has contents
#     dbutils.fs.rm(destination_path, recurse=True)
#     print(f"Deleted: {destination_path}")

# # 2. Recreate the destination directory
# dbutils.fs.mkdirs(destination_path)

# # 3. Copy all files from source to destination
# files = dbutils.fs.ls(source_path)

# for file in files:
#     src = file.path
#     dst = destination_path + file.name
#     dbutils.fs.cp(src, dst, recurse=True)
#     print(f"Copied: {src} -> {dst}")
