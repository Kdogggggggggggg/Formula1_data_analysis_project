# Databricks notebook source
raw_folder_path = 'dbfs:/internship/Kaelin/raw'
processed_folder_path = 'dbfs:/internship/Kaelin/processed'
presentation_folder_path = 'dbfs:/internship/Kaelin/presentation'
f1_processed_kg = 'f1_processed_kg'

# COMMAND ----------

# # Define source and destination folders
# source_path = "dbfs:/FileStore/2021-04-18/"
# destination_path = "dbfs:/internship/Kaelin/raw/2021-04-18/"

# # Create the destination directory if it doesn't exist
# dbutils.fs.mkdirs(destination_path)

# # List all files in the source folder
# files = dbutils.fs.ls(source_path)

# # Loop through and move each file
# for file in files:
#     src = file.path
#     dst = destination_path + file.name
#     dbutils.fs.mv(src, dst, recurse=True)
#     print(f"Moved: {src} -> {dst}")
