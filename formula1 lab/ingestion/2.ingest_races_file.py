# Databricks notebook source
# MAGIC %md
# MAGIC ##### step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")
#v_data_source  # print variablt

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, TimestampType

# COMMAND ----------

races_schema = StructType(fields = [StructField("raceId", IntegerType(), False),
                                       StructField("year", IntegerType(), True),
                                       StructField("round", IntegerType(), True),
                                       StructField("circuitId", IntegerType(), True),
                                       StructField("name", StringType(), True),
                                       StructField("date", DateType(), True),
                                       StructField("time", StringType(), True),
                                       StructField("url", StringType(), True),
                                      ])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_df.describe().show() # min/max values of each column

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Add ingestion date and race_timestamp to the dataframe
# MAGIC ##### and rename columns

# COMMAND ----------

races_new_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
.withColumn("ingestion_date", current_timestamp()) \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("circuitId", "circuit_id") \
.withColumn("data_source", lit(v_data_source))\
.withColumn("file_date", lit(v_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select Final Columns

# COMMAND ----------

races_final_df = races_new_df.select(col("race_id"), col("race_year"), col("round"),
                                        col("circuit_id"), col("name"),
                                        col("race_timestamp"), col("ingestion_date"),
                                    col("data_source"), col("file_date"))

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data out to parquet files

# COMMAND ----------

# races_final_df.write.mode("overwrite").partitionBy('race_year').parquet(f"{processed_folder_path}/races")
# races_final_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")
races_final_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# df = spark.read.parquet(f"{processed_folder_path}/races")
# df = spark.read.delta(f"{processed_folder_path}/races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*), file_date
# MAGIC from f1_processed.races
# MAGIC group by file_date

# COMMAND ----------

dbutils.notebook.exit("Success")
