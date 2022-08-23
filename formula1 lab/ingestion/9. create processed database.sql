-- Databricks notebook source
create database if not exists f1_processed
location "/mnt/formula1dfk/processed"

-- COMMAND ----------

desc database f1_raw; -- dbfs:/user/hive/warehouse/f1_raw.db
desc database f1_processed; -- dbfs:/mnt/formula1dfk/processed

-- COMMAND ----------

-- MAGIC %python
