# Databricks notebook source
'''
%sql
DROP DATABASE IF EXISTS watermark_db CASCADE; '''

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS watermark_db  
# MAGIC LOCATION '/mnt/raw-container/watermarkdb'

# COMMAND ----------

# MAGIC %sql
# MAGIC USE watermark_db ;
# MAGIC CREATE TABLE IF NOT EXISTS watermark_db.watermark_table (
# MAGIC TableName STRING,
# MAGIC CdcColumnName STRING,
# MAGIC WatermarkValue DATE, 
# MAGIC ActiveFlag STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/raw-container/watermarkdb'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended watermark_db.watermark_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert details of list of tables to be extracted based on cdc column

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO watermark_db.watermark_table
# MAGIC VALUES ('Table1', 'cdc_col1', '2024-04-19 00:00:00', 'Y'),
# MAGIC        ('Table2', 'cdc_col2', '2024-04-20 00:00:00', 'Y'),
# MAGIC        ('Table3', 'cdc_col3', '2024-04-21 00:00:00', 'N');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from watermark_db.watermark_table ;
