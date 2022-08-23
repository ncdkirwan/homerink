# Databricks notebook source
# MAGIC %md
# MAGIC ##### Step 1 - Read JSON file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema =StructType(fields=[StructField("forename", StringType(), True),
                                           StructField("surname", StringType(), True)
                               ])

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

drivers_schema =StructType(fields=[StructField("driverId", IntegerType(), False),
                                   StructField("driverRef", StringType(), True),
                                   StructField("number", IntegerType(), True),
                                   StructField("code", StringType(), True),
                                   StructField("name", name_schema, True),
                                   StructField("dob", DateType(), True),
                                   StructField("nationality", StringType(), True),
                                   StructField("url", StringType(), True),
                               ])

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# display(drivers_df)
# drivers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2 - Rename and add columns

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

driver_newCol_df = add_ingestion_date(drivers_df)

# COMMAND ----------

drivers_final_df = driver_newCol_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("driverRef", "driver_ref") \
.withColumn("name", concat(col("name.forename"), lit(' '), col("name.surname"))) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))\
.drop("url") \
.drop("name.forename") \
.drop("name.surname")

# COMMAND ----------

# display(drivers_final_df)

# COMMAND ----------

# drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")
drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")
# drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/drivers"))

# COMMAND ----------

dbutils.notebook.exit("Success")
