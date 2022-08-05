# Databricks notebook source
# MAGIC %md
# MAGIC 1. write data to delta lake(managed labels)
# MAGIC 2. write data to delta lake(external labels)
# MAGIC 3. read data from delta lake(Tables)
# MAGIC 4. read data from delta lake(file)

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists f1_demo
# MAGIC location '/mnt/formula1dfk/demo'

# COMMAND ----------

results_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1dfk/raw/2021-03-28/results.json")

# COMMAND ----------

#Managed table
results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

#file table
results_df.write.format("delta").mode("overwrite").save("/mnt/formula1dfk/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table f1_demo.results_external
# MAGIC using delta
# MAGIC location '/mnt/formula1dfk/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from f1_demo.results_managed

# COMMAND ----------

results_external_df = spark.read.format('delta').load("/mnt/formula1dfk/demo/results_external")
display(results_external_df)

# COMMAND ----------

#file table
results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update Delta Table
# MAGIC 2. Delete from Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC update f1_demo.results_managed
# MAGIC set points = 11-position
# MAGIC where position <= 10

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1dfk/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
# deltaTable.update(
#   condition = "position <= 10",
#   set = { "points": "21-position" }
# )
deltaTable.update(
  "position <= 10",  { "points": "21-position" } )

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.results_managed
# MAGIC where position > 10;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1dfk/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete("points =0")

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert using Merge

# COMMAND ----------

drivers_day_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1dfk/raw/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId", "dob", "name.forename", "name.surname")
display(drivers_day_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Day 1

# COMMAND ----------

drivers_day_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

# MAGIC %md
# MAGIC Day 2

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1dfk/raw/2021-03-28/drivers.json") \
.filter("driverId between 6 and 15") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))
display(drivers_day2_df)

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

# MAGIC %md
# MAGIC Day 3

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1dfk/raw/2021-03-28/drivers.json") \
.filter("driverId between 1 and 5 or driverId between 16 and 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))
display(drivers_day3_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_merge(
# MAGIC driverId int,
# MAGIC dob date,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC createddate timestamp,
# MAGIC updateddate timestamp
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_demo.drivers_merge dm
# MAGIC using drivers_day1 dd
# MAGIC on dm.driverId = dd.driverId
# MAGIC when matched then
# MAGIC   update set dm.dob = dd.dob,
# MAGIC     dm.forename = dd.forename,
# MAGIC     dm.surname = dd.surname,
# MAGIC     dm.updateddate = current_timestamp
# MAGIC when not matched then
# MAGIC   insert (driverId, dob,forename,surname,
# MAGIC     createddate, updateddate)
# MAGIC   values(driverId, dob,forename,surname, current_timestamp, current_timestamp)
# MAGIC   ;
# MAGIC select *
# MAGIC from f1_demo.drivers_merge
# MAGIC ;
# MAGIC -- drop table f1_demo.drivers_merge

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1dfk/demo/drivers_merge')

# Declare the predicate by using a SQL-formatted string.
deltaTable.alias("dm").merge(
  drivers_day3_df.alias("dd"), "dm.driverId = dd.driverId") \
.whenMatchedUpdate( set = {
  "dm.dob" : "dd.dob",
    "dm.forename" : "dd.forename",
    "dm.surname" : "dd.surname",
    "dm.updateddate" : "current_timestamp()"
}) \
.whenNotMatchedInsert(values = {
  "driverId" : "dd.driverId",
  "dob" : "dd.dob",
  "forename" : "dd.forename",
  "surname" : "dd.surname",
  "createddate" : "current_timestamp()",
  "updateddate" : "current_timestamp()"
}) \
.execute()




# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_demo.drivers_merge dm
# MAGIC using drivers_day2 dd
# MAGIC on dm.driverId = dd.driverId
# MAGIC when matched then
# MAGIC   update set dm.dob = dd.dob,
# MAGIC     dm.forename = dd.forename,
# MAGIC     dm.surname = dd.surname,
# MAGIC     dm.updateddate = current_timestamp
# MAGIC when not matched then
# MAGIC   insert (driverId, dob,forename,surname,
# MAGIC     createddate, updateddate)
# MAGIC   values(driverId, dob,forename,surname, current_timestamp, current_timestamp)
# MAGIC   ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from f1_demo.drivers_merge
# MAGIC order by driverId

# COMMAND ----------

# MAGIC %md
# MAGIC 1. history & versioning
# MAGIC 2. Time Travel
# MAGIC 3. Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC --use results from desc history statement
# MAGIC select *
# MAGIC from f1_demo.drivers_merge
# MAGIC -- version as of 2
# MAGIC timestamp as of '2022-08-03T13:47:58.000+0000'

# COMMAND ----------

df = spark.read.format("delta").option("timestamAsOf", '2022-08-03T13:47:58.000+0000').load("/mnt/formula1dfk/demo/drivers_merge")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Vaccum command

# COMMAND ----------

# MAGIC %sql
# MAGIC --cleans all data
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC vacuum f1_demo.drivers_merge retain 0 hours --remove any history

# COMMAND ----------

# MAGIC %sql
# MAGIC --use results from desc history statement
# MAGIC select *
# MAGIC from f1_demo.drivers_merge
# MAGIC version as of 4
# MAGIC -- timestamp as of '2022-08-03T13:47:58.000+0000'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete from f1_demo.drivers_merge where driverId = 1;
# MAGIC select *
# MAGIC from f1_demo.drivers_merge
# MAGIC -- version as 4
# MAGIC order by driverId 

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_demo.drivers_merge tgt
# MAGIC using f1_demo.drivers_merge version as of 4 src
# MAGIC    on (tgt.driverId = src.driverId)
# MAGIC when not matched then
# MAGIC   insert *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transactions Logs

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_txn(
# MAGIC driverId int,
# MAGIC dob date,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC createddate timestamp,
# MAGIC updateddate timestamp
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn
# MAGIC select * from f1_demo.drivers_merge
# MAGIC where driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn
# MAGIC select * from f1_demo.drivers_merge
# MAGIC where driverId = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from  f1_demo.drivers_txn
# MAGIC where driverId = 1;

# COMMAND ----------

for driver_id in range(3,20):
  spark.sql(f"""INSERT INTO f1_demo.drivers_txn
                select * from f1_demo.drivers_merge
                where driverId = {driver_id}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn
# MAGIC select * from f1_demo.drivers_merge
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_convert_to_delta(
# MAGIC driverId int,
# MAGIC dob date,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC createddate timestamp,
# MAGIC updateddate timestamp
# MAGIC )
# MAGIC using parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_convert_to_delta
# MAGIC select * from f1_demo.drivers_merge
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta f1_demo.drivers_convert_to_delta

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/formula1dfk/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta parquet.`/mnt/formula1dfk/demo/drivers_convert_to_delta_new`