# Databricks notebook source
raw_name = "raw"
processed_name = "processed"
presentation_name = "presentation"
demo_name = "demo"
path_name = "/mnt/formula1dfk"

# COMMAND ----------

raw_folder_path = f"{path_name}/{raw_name}"
processed_folder_path = f"{path_name}/{processed_name}"
presentation_folder_path = f"{path_name}/{presentation_name}"
demo_folder_path = f"{path_name}/{demo_name}"

# COMMAND ----------

storage_account_name = "formula1dfk"
scope = "formula1-scope"
client_id = dbutils.secrets.get(scope=f"{scope}", key="db-app-client-id")      
tenant_id = dbutils.secrets.get(scope=f"{scope}", key="db-app-tenant-id")      
client_secret = dbutils.secrets.get(scope=f"{scope}", key="db-app-client-secret")  

# COMMAND ----------

configs = {"fs.azure.account.auth.type" : "OAuth",
           "fs.azure.account.oauth.provider.type" : "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id" : f"{client_id}",
           "fs.azure.account.oauth2.client.secret" : f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint" : f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
          }
