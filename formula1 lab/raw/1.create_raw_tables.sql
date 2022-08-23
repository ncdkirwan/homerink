-- Databricks notebook source
create database if not exists f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create circuits file

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(
circuitId int,
circuitRef string,
name string,
location string,
country string,
lat double,
long double,
alt int,
url string
)
using csv
options (path "/mnt/formula1dfk/raw/circuits.csv", header true)

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races(
raceId int,
year int,
round int,
circuitId int,
name string,
date date,
time string,
url string
)
using csv
options (path "/mnt/formula1dfk/raw/races.csv", header true)
;
select *
from f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create tables for JSON Files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Constructors tables
-- MAGIC * Single Line JSON
-- MAGIC * Complex structure

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors(
constructorId INT, 
constructorRef String, 
name STRING, 
nationality string, 
url string
)
using JSON
options (path = "/mnt/formula1dfk/raw/constructors.json")
;
select *
from f1_raw.constructors

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(
driverId int,
driverRef string,
number int,
code string,
name STRUCT<forename: string, surname string>,
dob date,
nationality string,
url string
)
using JSON
options (path = "/mnt/formula1dfk/raw/drivers.json")
;
select *
from f1_raw.drivers

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results(
resultId int,
raceId int,
driverId int,
constructorId int,
number int,
grid int,
position int,
positionText string,
positionOrder int,
points float,
laps int,
time string,
milliseconds int,
fastestLap int,
rank int,
fastestLapTime string,
fastestLapSpeed float,
statusId int
)
using JSON
options (path = "/mnt/formula1dfk/raw/results.json")
;
select *
from f1_raw.results

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(
raceId int,
driverId int,
stop string,
lap int,
time string,
duration string,
milliseconds int
)
using JSON
options (path = "/mnt/formula1dfk/raw/pit_stops.json", multiLine True)
;
select *
from f1_raw.pit_stops

-- COMMAND ----------

drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times(
raceId int,
driverId int,
lap int,
position int,
time string,
milliseconds int
)
using csv
options (path = "/mnt/formula1dfk/raw/lap_times/lap_times_split*.csv")
;
select *
from f1_raw.lap_times

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
qualifyId int,
raceId int,
driverId int,
constructorId int,
number int,
position int,
q1 string,
q2 string,
q3 string
)
using json
options (path = "/mnt/formula1dfk/raw/qualifying/qualifying_split*.json", multiLine true)
;
select *
from f1_raw.qualifying
