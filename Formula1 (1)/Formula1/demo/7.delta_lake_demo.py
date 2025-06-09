# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo_kg
# MAGIC LOCATION "dbfs:/internship/Kaelin/demo"

# COMMAND ----------

results_df = spark.read \
    .option("inferSchema", True) \
    .json(f"dbfs:/internship/Kaelin/raw/2021-03-28/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data to delta lake

# COMMAND ----------

results_df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("f1_demo_kg.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_kg.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to file location

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("dbfs:/internship/Kaelin/demo/results_external")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo_kg.results_external
# MAGIC USING DELTA 
# MAGIC LOCATION "dbfs:/internship/Kaelin/demo/results_external"
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_kg.results_external

# COMMAND ----------

results_external_df = spark.read.format("delta").load("dbfs:/internship/Kaelin/demo/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

results_df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .partitionBy("constructorId") \
    .saveAsTable("f1_demo_kg.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo_kg.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update and Delete From Delta Tables

# COMMAND ----------

# MAGIC %sql 
# MAGIC UPDATE f1_demo_kg.results_managed
# MAGIC SET points = 11 - position 
# MAGIC WHERE position <= 10 -- sets the other rows to 0? 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo_kg.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "dbfs:/internship/Kaelin/demo/results_managed")

deltaTable.update("position <= 10", {"points": '21 - position'})

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Delete

# COMMAND ----------

# MAGIC %sql 
# MAGIC delete from f1_demo_kg.results_managed
# MAGIC Where position > 10;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "dbfs:/internship/Kaelin/demo/results_managed")

deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo_kg.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC #### Upsert Using Merge
# MAGIC ##### insert new, update existing, and delete if reqested

# COMMAND ----------

drivers_day1_df = spark.read \
    .option("inferSchema", True) \
    .json(f"{raw_folder_path}/2021-03-28/drivers.json") \
    .filter("driverId <= 10") \
    .select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1") 

# COMMAND ----------

from pyspark.sql.functions import upper 
drivers_day2_df = spark.read \
    .option("inferSchema", True) \
    .json(f"{raw_folder_path}/2021-03-28/drivers.json") \
    .filter("driverId BETWEEN 6 AND 15") \
    .select("driverId", "dob", upper("name.forename").alias('forename'), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

from pyspark.sql.functions import upper 
drivers_day3_df = spark.read \
    .option("inferSchema", True) \
    .json(f"{raw_folder_path}/2021-03-28/drivers.json") \
    .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
    .select("driverId", "dob", upper("name.forename").alias('forename'), upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTs f1_demo_kg.drivers_merge (
# MAGIC   driverId int, 
# MAGIC   dob DATE, 
# MAGIC   forename string,
# MAGIC   surname string, 
# MAGIC   createDate DATE, 
# MAGIC   updateDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo_kg.drivers_merge as tgt
# MAGIC USING drivers_day1 as upd
# MAGIC ON tgt.driverId = upd.driverId 
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET tgt.dob = upd.dob,
# MAGIC            tgt.forename = upd.forename,
# MAGIC            tgt.surname = upd.surname,
# MAGIC            tgt.updateDate = current_timestamp()
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT (driverId, dob, forename, surname, createDate) VALUES (upd.driverId, upd.dob, upd.forename, upd.surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo_kg.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo_kg.drivers_merge as tgt
# MAGIC USING drivers_day2 as upd
# MAGIC ON tgt.driverId = upd.driverId 
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET tgt.dob = upd.dob,
# MAGIC            tgt.forename = upd.forename,
# MAGIC            tgt.surname = upd.surname,
# MAGIC            tgt.updateDate = current_timestamp()
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT (driverId, dob, forename, surname, createDate) VALUES (upd.driverId, upd.dob, upd.forename, upd.surname, current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo_kg.drivers_merge

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "dbfs:/internship/Kaelin/demo/drivers_merge")

deltaTable.alias("tgt") \
    .merge(
        drivers_day3_df.alias("src"),
        "tgt.driverId = src.driverId") \
    .whenMatchedUpdate(set = {"dob": "src.dob", "forename": "src.forename", "surname": "src.surname", "updateDate": "current_timestamp()"}) \
    .whenNotMatchedInsert(values = {"driverId": "src.driverId", "dob": "src.dob", "forename": "src.forename", "surname": "src.surname", "createDate": "current_timestamp()"}) \
    .execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo_kg.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC #### History, Versioning, TimeTravel and Vaccum

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESC HISTORY f1_demo_kg.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE f1_demo_kg.drivers_merge;
# MAGIC
# MAGIC SELECT * FROM f1_demo_kg.drivers_merge VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo_kg.drivers_merge TIMESTAMP AS OF '2025-06-02T17:01:51.000+00:00'

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", "2025-06-02T17:01:51.000+00:00").load("dbfs:/internship/Kaelin/demo/drivers_merge")
display(df)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC vacuum f1_demo_kg.drivers_merge RETAIN 0 hours
# MAGIC      
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo_kg.drivers_merge
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo_kg.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo_kg.drivers_merge WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo_kg.drivers_merge as tgt
# MAGIC USING f1_demo_kg.drivers_merge VERSION AS OF 5 as upd
# MAGIC ON (tgt.driverId = upd.driverId)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo_kg.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo_kg.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transaction Logs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo_kg.drivers_txn (
# MAGIC   driverId INT, 
# MAGIC   dob DATE, 
# MAGIC   forename STRING, 
# MAGIC   surname STRING, 
# MAGIC   createDate DATE, 
# MAGIC   updateDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into f1_demo_kg.drivers_txn
# MAGIC select * from f1_demo_kg.drivers_merge where driverId = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo_kg.drivers_txn

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into f1_demo_kg.drivers_txn
# MAGIC select * from f1_demo_kg.drivers_merge where driverId = 2

# COMMAND ----------

for i in range(3, 20):
    # 1 log created per insert 
      spark.sql(f"""INSERT INTO f1_demo_kg.drivers_txn
                    SELECT * FROM f1_demo_kg.drivers_merge
                    WHERE driverId = {i}""")
      

# COMMAND ----------

# MAGIC %sql
# MAGIC -- makes 1 log file for 20 records inserted 
# MAGIC INSERT INTO f1_demo_kg.drivers_txn
# MAGIC SELECT * FROM f1_demo_kg.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo_kg.drivers_convert_to_delta (
# MAGIC   driverId INT, 
# MAGIC   dob DATE, 
# MAGIC   forename STRING, 
# MAGIC   surname STRING, 
# MAGIC   createDate DATE, 
# MAGIC   updateDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo_kg.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo_kg.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC -- parquet files still exist, and a delta log is added with checkpoint
# MAGIC CONVERT TO DELTA f1_demo_kg.drivers_convert_to_delta

# COMMAND ----------

df = spark.table("f1_demo_kg.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("dbfs:/internship/Kaelin/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`dbfs:/internship/Kaelin/demo/drivers_convert_to_delta_new`