# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuts.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")
v_data_source  # print variablt
v_file_date

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields = [StructField("circuitId", IntegerType(), False),
                                       StructField("circuitRef", StringType(), False),
                                       StructField("name", StringType(), False),
                                       StructField("location", StringType(), False),
                                       StructField("country", StringType(), False),
                                       StructField("lat", DoubleType(), False),
                                       StructField("lng", DoubleType(), False),
                                       StructField("alt", IntegerType(), False),
                                       StructField("url", StringType(), False),
                                      ])

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show() # min/max values of each column

# COMMAND ----------

# circuits_df.show() # truncates columns
display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select only the required columns

# COMMAND ----------

# circuits_selected_df = circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

# circuits_selected_df = circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef,circuits_df.name,
#                                           circuits_df.location,circuits_df.country,circuits_df.lat,circuits_df.lng,circuits_df.alt)

# COMMAND ----------

# circuits_selected_df = circuits_df.select(circuits_df["circuitId"],circuits_df["circuitRef"],circuits_df["name"],
#                                           circuits_df["location"],circuits_df["country"],circuits_df["lat"],circuits_df["lng"],circuits_df["alt"])

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),
                                          col("location"),col("country"),
                                          col("lat"),col("lng"),col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)
# \
# .withColumn("env", lit("Production")) # how to add string value 

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write data to datalake as parquet

# COMMAND ----------

# circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")
# circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended f1_processed.circuits

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read loaded data

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dfk/processed

# COMMAND ----------

# df = spark.read.parquet(f"{processed_folder_path}/circuits")
# display(spark.read.parquet(f"{processed_folder_path}/circuits"))


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# display(dbutils.fs.mounts())
# %fs
# ls /mnt/formula1dfk/raw
