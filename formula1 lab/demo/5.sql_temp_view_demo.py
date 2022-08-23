# Databricks notebook source
# MAGIC %sql
# MAGIC select *
# MAGIC from v_race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from global_temp.gv_race_results

# COMMAND ----------

display(spark.sql("select * from global_temp.gv_race_results"))
