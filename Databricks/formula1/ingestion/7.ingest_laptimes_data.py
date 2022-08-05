# Databricks notebook source
# MAGIC %md
# MAGIC #### ingest Lap Time data files

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")
#v_data_source  # print variablt

# COMMAND ----------

lapttimes_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])


# COMMAND ----------

laptime_df = spark.read \
.schema(lapttimes_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/lap_times/lap_times_split*.csv")

# COMMAND ----------

# display(laptime_df)
# laptime_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

laptime_final_df = laptime_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# laptime_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")
# laptime_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")
# overwrite_partition(laptime_final_df, 'f1_processed', 'lap_times', 'race_id')
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(laptime_final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Success")