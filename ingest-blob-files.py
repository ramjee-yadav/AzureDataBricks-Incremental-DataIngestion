# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount the target raw-container and initialize the target adls path

# COMMAND ----------

#%run "./mount_adls_container"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Move the files from blob storage to ADLS

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .load(sourcefile_mount)

# COMMAND ----------

# Set the ADLS target path for extracted data from source_blob
adls_target_path = f"{raw_incremental_mount_point}/source_files"
print(adls_target_path)

# COMMAND ----------

#df.write.format("csv").save(adls_target_path)
df.writeStream.trigger(once=True).format("csv").option(
    "checkpointLocation", autoloader_checkpoint
).start(adls_target_path)
