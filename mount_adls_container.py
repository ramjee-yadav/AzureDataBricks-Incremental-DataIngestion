# Databricks notebook source
# MAGIC %md
# MAGIC ##### Get Secretes of service app (access to Azure ADLS granted to it)

# COMMAND ----------

# List all the secret scopes
dbutils.secrets.listScopes()

# COMMAND ----------

adb_secrete_scope_name='ramg-de-scope'
tenant_id=dbutils.secrets.get(scope = adb_secrete_scope_name, key = 'ramg-de-app-tenant-id')
client_id=dbutils.secrets.get(scope = adb_secrete_scope_name, key = 'ramg-de-app-client-id')
client_secret=dbutils.secrets.get(scope = adb_secrete_scope_name, key = 'ramg-de-app-client-secret')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Mount ADLS to ADB

# COMMAND ----------

storage_account_name = "rawincrementaldata"
container_name = "raw-container"
raw_incremental_folder='incremental-data-tables'

watermark_folder='watermarkdb'

sourcefile_stoarge_account_name="ramgdesourcestorage"
sourcefile_container='sourcefile-container'
input_folder="input"

# COMMAND ----------

# MAGIC %md
# MAGIC #### ADB Mount Paths

# COMMAND ----------

raw_incremental_mount_point = f"/mnt/{container_name}/{raw_incremental_folder}"
watermark_mount_point = f"/mnt/{container_name}/{watermark_folder}"
autoloader_checkpoint = f"/mnt/{container_name}/autoloader_checkpoint"

sourcefile_mount= f"/mnt/{sourcefile_container}/{input}"
print(raw_incremental_mount_point)
print(watermark_mount_point)
print(sourcefile_mount)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### mount raw-incremental-load folder

# COMMAND ----------

# Check if mount point exists
if not any(mount.mountPoint == raw_incremental_mount_point for mount in dbutils.fs.mounts()):
    # Mount the container
  configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
  print('Configs initialized')
  dbutils.fs.mount(
    source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{raw_incremental_folder}",
    mount_point=raw_incremental_mount_point,
    extra_configs=configs)
  print('raw_incremental_mount_point mounted successfully.')  
else:
  print('Mount already created')

# COMMAND ----------

# Check if mount point exists
if not any(mount.mountPoint == watermark_mount_point for mount in dbutils.fs.mounts()):
    # Mount the container
  configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
  print('Configs initialized')
  dbutils.fs.mount(
    source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{watermark_folder}",
    mount_point=watermark_mount_point,
    extra_configs=configs)
  print('watermark_mount_point mounted successfully.')  
else:
  print('Mount for watermark already created')

# COMMAND ----------

# To unmount the container, uncomment the following line
#dbutils.fs.unmount(watermark_mount_point)

# COMMAND ----------

display(dbutils.fs.mounts())


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/raw-container

# COMMAND ----------

# MAGIC %md
# MAGIC Mount input source file stoarge

# COMMAND ----------

# Check if mount point exists
if not any(mount.mountPoint == source_blob_mount for mount in dbutils.fs.mounts()):
    # Mount the container
  configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
  print('Configs initialized')
  dbutils.fs.mount(
    source=f"abfss://{sourcefile_container}@{sourcefile_stoarge_account_name}.dfs.core.windows.net/{input}",
    mount_point=sourcefile_mount,
    extra_configs=configs)
  print('sourcefile_container mounted successfully.')  
else:
  print('Mount for input blob storage already created')
