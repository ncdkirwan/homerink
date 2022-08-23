# Databricks notebook source
# %sql
# use f1_presentation

# COMMAND ----------

dbutils.widgets.text("p_file_date", '2021-03-28')
v_file_date = dbutils.widgets.get("p_file_date");

# COMMAND ----------

spark.sql(f""" 
  create table if not exists f1_presentation.calculated_race_results (
    race_year int,
    team_name string,
    driver_id int,
    driver_name string,
    race_id int,
    position int,
    points int,
    calculated_points int,
    created_date timestamp,
    updated_date timestamp
  )
  using delta
""")

# COMMAND ----------

spark.sql(f""" 
create or replace temp view race_results_updated
as
select rs.race_year, c.name as team_name, d.driver_id, d.name as driver_name
  , rs.race_id, r.position, r.points
  , case 
    when r.position >= 10 then 0
    when r.position is null then 0
    else (11 - r.position)
    end calculated_points
from f1_processed.results r join f1_processed.drivers d on r.driver_id = d.driver_id
  join f1_processed.constructors c on r.constructor_id = c.constructor_id
  join f1_processed.races rs on r.race_id = rs.race_id
where r.position <= 10
  and r.file_date = '{v_file_date}'
""")

# COMMAND ----------

spark.sql(f"""
  MERGE INTO f1_presentation.calculated_race_results tgt
  USING race_results_updated src
  ON (tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id)
  WHEN MATCHED THEN
    UPDATE SET tgt.position = src.position,
               tgt.points = src.points,
               tgt.calculated_points = src.calculated_points,
               tgt.updated_date = current_timestamp
  WHEN NOT MATCHED THEN 
    INSERT (race_year, team_name, driver_id, driver_name, race_id
        , position, points, calculated_points, created_date, updated_date ) 
    VALUES(src.race_year, src.team_name, src.driver_id, src.driver_name, src.race_id
          , src.position, src.points, src.calculated_points, current_timestamp, current_timestamp)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT COUNT(1) FROM race_results_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT COUNT(1) FROM f1_presentation.calculated_race_results;
