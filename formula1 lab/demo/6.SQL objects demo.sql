-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Lesson Objectives
-- MAGIC 1. Spark SQL documentation
-- MAGIC 1. Create Database demo
-- MAGIC 1. Data tab in the UI
-- MAGIC 1. SHOW command
-- MAGIC 1. DESCRIBE command
-- MAGIC 1. Find the current database

-- COMMAND ----------

create database if not exists demo;

-- COMMAND ----------

-- create table if not exists
show databases;
show tables in default;

-- COMMAND ----------

-- desc database demo
describe database demo

-- COMMAND ----------

describe database extended demo

-- COMMAND ----------

select current_database();

-- COMMAND ----------

show tables;
show tables in demo;

-- COMMAND ----------

use demo; --switch databases
show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Learning Objectives
-- MAGIC 1. Create managed table using python
-- MAGIC 2. Create managed table using SQL
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

use demo;
show tables;
-- select * from demo.race_results_python where race_year = 2020;
-- describe extended race_results_python;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Managed table

-- COMMAND ----------

create table race_results_sql
as 
select * from demo.race_results_python where race_year = 2020;

-- COMMAND ----------

describe extended race_results_sql

-- COMMAND ----------

-- drop table demo.race_results_sql;
show tables in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Learning Objectives
-- MAGIC 1. Create external table using python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effect of dropping a managed table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ex_py").saveAsTable("demo.race_results_python_py")

-- COMMAND ----------

desc extended demo.race_results_python_py


-- COMMAND ----------

create table demo.race_results_ext_sql(
  race_year 		int,
  race_name 		string,
  race_date  		timestamp,
  circuit_location 	string,
  driver_name 		string,
  driver_number 	int,
  drive_nationality	string,
  team 				string,
  grid 				int,
  fastest_lap 		int,
  race_time 		string,
  points 			float,
  position      	int,
  created_date 		timestamp
)
using parquet
location "/mnt/formula1dfk/presentation/race_results_ext_sql"



-- COMMAND ----------

-- desc demo.race_results_ext_sql;
show tables

-- COMMAND ----------

-- insert into demo.race_results_ext_sql
-- select *
-- from demo.race_results_python_py
-- where race_year = 2020
;
select *  from demo.race_results_ext_sql;
-- drop table demo.race_results_ext_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Views on Tables
-- MAGIC 1. Create Temp View
-- MAGIC 2. Create Global Temp View
-- MAGIC 3. Create Permanent View

-- COMMAND ----------

use demo;
create or replace temp view v_race_results
as
select *
from demo.race_results_python
where race_year = 2018

-- COMMAND ----------

use demo;
create or replace global temp view gv_race_results
as
select *
from demo.race_results_python
where race_year = 2012

-- COMMAND ----------

use demo;
create or replace view demo.pv_race_results
as
select *
from demo.race_results_python
where race_year = 2000
;

-- COMMAND ----------

select *
from v_race_results;

select *
from global_temp.gv_race_results
;
select *
from demo.pv_race_results
;
-- show tables in demo;
