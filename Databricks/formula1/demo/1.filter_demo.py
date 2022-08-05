# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

# races_filtered_df = races_df.filter("race_year = 2019 and round <= 5")  # SQL fashion - limit to 5 rows
# races_filtered_df = races_df.filter((races_df["race_year"] == 2019) &  (races_df["round"] <= 5)) # python fashion
races_filtered_df = races_df.where((races_df["race_year"] == 2019) &  (races_df["round"] <= 5)) # python fashion

# COMMAND ----------

display(races_filtered_df)