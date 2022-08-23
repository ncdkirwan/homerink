-- Databricks notebook source
show databases;

-- COMMAND ----------

select current_database();
use f1_processed;
show tables;

-- COMMAND ----------

select *
from f1_processed.drivers
where nationality = 'British'
  and dob >= '1990-01-01'
-- limit 10

-- COMMAND ----------

select name, dob as date_of_birth
from f1_processed.drivers
where nationality = 'British'
  and dob >= '1990-01-01'
order by dob desc

-- COMMAND ----------

select *
from f1_processed.drivers

order by nationality, dob desc

-- COMMAND ----------

select name, dob as date_of_birth, nationality
from f1_processed.drivers
where (nationality = 'British'
  and dob >= '1990-01-01')
  or nationality = 'Indian'
order by dob desc

-- COMMAND ----------

desc f1_processed.drivers
