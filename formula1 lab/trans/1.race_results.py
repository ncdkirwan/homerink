# Databricks notebook source
dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
.withColumnRenamed('name', 'race_name') \
.withColumnRenamed('year', 'race_year') \
.withColumnRenamed('race_timestamp', 'race_date')
circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
.withColumnRenamed('name', 'circuit_name') \
.withColumnRenamed('location', 'circuit_location') \
.filter("circuit_id < 70")
drivers_df =  spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
.withColumnRenamed('name', 'driver_name') \
.withColumnRenamed('number', 'driver_number') \
.withColumnRenamed('nationality', 'driver_nationality') 
constructors_df =  spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
.withColumnRenamed('name', 'team') 
results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
.filter(f"file_date = '{v_file_date}'") \
.withColumnRenamed('time', 'race_time') \
.withColumnRenamed("race_id", "results_race_id") \
.withColumnRenamed("file_date", "results_file_date")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)
race_results_df = results_df.join(race_circuits_df, results_df.results_race_id == race_circuits_df.race_id ) \
            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id) \
.select(race_circuits_df.race_id, race_circuits_df.race_year, race_circuits_df.race_name, race_circuits_df.race_date, race_circuits_df.circuit_location, \
       drivers_df.driver_name, drivers_df.driver_number, drivers_df.driver_nationality, constructors_df.team, results_df.grid, \
        results_df.fastest_lap, results_df.race_time, results_df.points, results_df.position, results_df.results_file_date)

display(race_results_df)

# COMMAND ----------

final_df = race_results_df.withColumn("created_date", current_timestamp()) \
.withColumnRenamed("results_file_date", "file_date")
# display(final_df.filter("race_year = 2020 and race_name ='Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- convert to delta f1_presentation.race_results partitioned by (race_id int)

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")
# overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_id')
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_name = src.driver_name"
merge_delta_data(final_df, 'f1_processed', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dfk/presentation

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(*)
# MAGIC from f1_presentation.race_results
# MAGIC group by race_id
# MAGIC order by 1 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table f1_presentation.race_results
