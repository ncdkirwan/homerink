# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Product driver standings

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'") 

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

team_standings_df = race_results_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"), \
     count(when(col("position") ==1, True )).alias("wins"))


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc, col

team_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = team_standings_df.withColumn("rank", rank().over(team_rank_spec))


# COMMAND ----------

# MAGIC %sql
# MAGIC -- convert to delta f1_presentation.team_standings partitioned by (race_year int)

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/team_standings")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.team_standings")
# overwrite_partition(final_df, 'f1_presentation', 'team_standings', 'race_year')
# display(spark.read.parquet(f"{presentation_folder_path}/team_standings"))
merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_processed', 'team_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_year, count(*)
# MAGIC from f1_presentation.team_standings
# MAGIC group by race_year
# MAGIC order by 1 desc;
# MAGIC 
# MAGIC -- drop table f1_presentation.team_standings
