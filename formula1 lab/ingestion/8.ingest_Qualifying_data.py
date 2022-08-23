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

qual_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True)
                                     ])


# COMMAND ----------

qual_df = spark.read \
.schema(qual_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/qualifying/qualifying_split*.json")

# COMMAND ----------

display(qual_df)
# qual_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qual_final_df = qual_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# qual_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")
# qual_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")
# overwrite_partition(qual_final_df, 'f1_processed', 'qualifying', 'race_id')
merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qual_final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")
