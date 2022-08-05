# Databricks notebook source
# spark.read.json("/mnt/formula1dfk/raw/2021-03-21/results.json").createOrReplaceTempView("results_cutover")
# spark.read.json("/mnt/formula1dfk/raw/2021-03-28/results.json").createOrReplaceTempView("results_w1")
# spark.read.json("/mnt/formula1dfk/raw/2021-04-18/results.json").createOrReplaceTempView("results_w2")

# COMMAND ----------

# %sql
# select race_id, count(*) cnt
# from results_cutover
# group by race_id
# order by 1 desc

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read JSON file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")
#v_data_source  # print variablt

# COMMAND ----------

results_schema =StructType(fields=[StructField("resultId", IntegerType(), False),
                                   StructField("raceId", IntegerType(), True),
                                   StructField("driverId", IntegerType(), True),
                                   StructField("constructorId", IntegerType(), True),
                                   StructField("number", IntegerType(), True),
                                   StructField("grid", IntegerType(), True),
                                   StructField("position", IntegerType(), True),
                                   StructField("positionText", StringType(), True),
                                   StructField("positionOrder", IntegerType(), True),
                                   StructField("points", FloatType(), True),
                                   StructField("laps", IntegerType(), True),
                                   StructField("time", StringType(), True),
                                   StructField("milliseconds", IntegerType(), True),
                                   StructField("fastestLap", IntegerType(), True),
                                   StructField("rank", IntegerType(), True),
                                   StructField("fastestLapTime", StringType(), True),
                                   StructField("fastestLapSpeed", FloatType(), True),
                                   StructField("statusId", IntegerType(), True)
                               ])


# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename and add columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

results_final_df = results_df.withColumnRenamed("resultId", "result_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("positionText", "position_text") \
.withColumnRenamed("positionOrder", "position_order") \
.withColumnRenamed("fastestLap", "fastest_lap") \
.withColumnRenamed("fastestLapTime", "fastest_lap_time") \
.withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date)) \
.drop(col("statusId"))

# COMMAND ----------

# display(results_final_df)

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### method 1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
# #     print(race_id_list.race_id)
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER table f1_processed.results DROP if exists partition (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("overwrite").partitionBy('race_id').parquet(f"{processed_folder_path}/results")
# results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table f1_processed.results

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

# results_final_df = results_final_df.select("result_id", "driver_id", "constructor_id", "number", "grid", "position"
#                                           , "position_text", "position_order", "points", "laps", "time", "milliseconds"
#                                           , "fastest_lap", "rank", "fastest_lap_time", "fastest_lap_speed", "data_source"
#                                           , "file_date", "ingestion_date", "race_id")
# the partition column MUST be the last column when using the InsertInto methodology

# COMMAND ----------

# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     results_final_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:        
#     results_final_df.write.mode("overwrite").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')
merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select *
# MAGIC -- from f1_processed.results r inner join 
# MAGIC -- (
# MAGIC select race_id, driver_id, count(*) as cnt
# MAGIC from f1_processed.results
# MAGIC group by race_id, driver_id
# MAGIC having count(*)>1
# MAGIC -- ) d on r.race_id = d.race_id and r.driver_id = d.driver_id
# MAGIC -- order by r.race_id desc, r.driver_id

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete from f1_processed.results
# MAGIC -- where file_date = '2021-03-21'
# MAGIC drop table f1_processed.results