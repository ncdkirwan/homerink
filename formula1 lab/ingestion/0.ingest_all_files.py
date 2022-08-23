# Databricks notebook source
data_source = "Ergast API"
fileDates = '2021-04-18'

# COMMAND ----------

v_result = dbutils.notebook.run("1.ingest_circuts_file", 0, {"p_data_source" : f"{data_source}", "p_file_date" : f"{fileDates}"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_races_file", 0, {"p_data_source" : f"{data_source}", "p_file_date" : f"{fileDates}"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_constructors_data", 0, {"p_data_source" : f"{data_source}", "p_file_date" : f"{fileDates}"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_drivers_data", 0, {"p_data_source" : f"{data_source}", "p_file_date" : f"{fileDates}"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingest_results_data", 0, {"p_data_source" : f"{data_source}", "p_file_date" : f"{fileDates}"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_pitstops_data", 0, {"p_data_source" : f"{data_source}", "p_file_date" : f"{fileDates}"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_laptimes_data", 0, {"p_data_source" : f"{data_source}", "p_file_date" : f"{fileDates}"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest_Qualifying_data", 0, {"p_data_source" : f"{data_source}", "p_file_date" : f"{fileDates}"})

# COMMAND ----------

v_result

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(*)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc
