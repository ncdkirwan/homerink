# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019") \
.withColumnRenamed('name', 'race_name')
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.withColumnRenamed('name', 'circuit_name') \
.filter("circuit_id < 70")

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

race_circuits_df.select("circuit_name").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Outer Joins

# COMMAND ----------

#left outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)
display(race_circuits_df)

# COMMAND ----------

#right outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

display(race_circuits_df)

# COMMAND ----------

#full outer join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round) \
.orderBy("circuit_name")

display(race_circuits_df)

# COMMAND ----------

#semi outer join, only returns records from left dataframe
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi") \
.orderBy("circuit_name")

display(race_circuits_df)

# COMMAND ----------

#anti outer join, only returns records that dont match criteria
race_circuits_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "anti") \


display(race_circuits_df)

# COMMAND ----------

#cross join, every record
race_circuits_df = races_df.crossJoin(circuits_df)

display(race_circuits_df)
race_circuits_df.count()

# COMMAND ----------

int(races_df.count()) * int(circuits_df.count())
