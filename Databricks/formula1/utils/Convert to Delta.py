# Databricks notebook source
# MAGIC %md 
# MAGIC Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- convert to delta f1_processed.circuits;
# MAGIC -- convert to delta f1_processed.constructors;
# MAGIC -- convert to delta f1_processed.drivers;
# MAGIC -- convert to delta f1_processed.lap_times;
# MAGIC -- convert to delta f1_processed.pit_stops;
# MAGIC -- convert to delta f1_processed.qualifying;
# MAGIC convert to delta f1_processed.races;
# MAGIC -- convert to delta f1_processed.results;