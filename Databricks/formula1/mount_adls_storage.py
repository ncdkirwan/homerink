# Databricks notebook source
# dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.list('formula1-scope')

# COMMAND ----------

# dbutils.secrets.get(scope ='formula1-scope', key ="db-app-client-id")

# COMMAND ----------

# for x in dbutils.secrets.get(scope="formula1-scope", key="db-app-client-id"):
#     print(x)

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

# COMMAND ----------

def unmount_adls(container_name):
    dbutils.fs.unmount(container_name)

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs
    )

# COMMAND ----------

unmount_adls("/mnt/formula1dfk/processed")

# COMMAND ----------

unmount_adls("/mnt/formula1dfk/raw")

# COMMAND ----------

mount_adls("raw")

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dfk/raw")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dfk/processed")

# COMMAND ----------

dbutils.fs.mounts()
