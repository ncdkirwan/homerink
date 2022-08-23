# Databricks notebook source
# MAGIC %run "./includes/common_functions"

# COMMAND ----------

# MAGIC %run "./includes/configuration"

# COMMAND ----------

unmount_adls(f"{presentation_folder_path}")

# COMMAND ----------

mount_adls("presentation")


# COMMAND ----------

dbutils.fs.ls(f"{presentation_folder_path}")

# COMMAND ----------

unmount_adls(f"{demo_folder_path}")

# COMMAND ----------

mount_adls(f"{demo_name}")

# COMMAND ----------

dbutils.fs.mounts()

