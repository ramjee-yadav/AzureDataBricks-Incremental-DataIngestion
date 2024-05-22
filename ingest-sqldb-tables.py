# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount the target raw-container and initialize the target adls path

# COMMAND ----------

# MAGIC %run "./mount_adls_container"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize connection details

# COMMAND ----------

# Set the connection details
adb_secrete_scope_name='ramg-de-scope'
Connurl=dbutils.secrets.get(scope = adb_secrete_scope_name, key = 'ramg-de-azure-sql-conn-url')
username=dbutils.secrets.get(scope = adb_secrete_scope_name, key = 'ramg-de-username')
pw=dbutils.secrets.get(scope = adb_secrete_scope_name, key = 'ramg-de-pw')

# Set the ADLS path
adls_target_path = raw_incremental_mount_point

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fetch table list to be loaded and their watermark values

# COMMAND ----------

df_watermark=spark.sql(f"""SELECT TableName,CdcColumnName, WatermarkValue FROM watermark_db.watermark_table""")
display(df_watermark)  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the respective tables one by one based on cdcColumnName and WatermarkValue

# COMMAND ----------

# Iterate over each row in df_watermark
for table_list in df_watermark.collect():
    # Extract the necessary values from df_watermark
    table_name =table_list['TableName']
    cdc_column_name = table_list['CdcColumnName']
    watermark_value = table_list['WatermarkValue']
    
    # Build the SQL query using the extracted values
    query = f"SELECT * FROM {table_name} WHERE {cdc_column_name} >= '{watermark_value}'"
    
    # Print the query
    print(query)

    # Read data from Azure SQL Database
    df_source_table = spark.read \
        .format("jdbc") \
        .option("url", Connurl) \
        .option("query", query) \
        .option("user", username) \
        .option("password", pw) \
        .load()
    
    # Fetch max datetime from a given table    
    max_date = df_source_table.selectExpr(f"MAX({cdc_column_name})").first()[0]
    print(f"source data from {table_name} fetched,and max date is {max_date}")
    
    # Write data to ADLS
    df_source_table.write \
        .format("csv") \
        .mode("overwrite") \
        .save(f"{adls_target_path}/{table_name}")        
    print(f"source data from {table_name} loaded to {adls_target_path}/{table_name}")    

    # Update watermark table
    sql_qry=f"""UPDATE watermark_db.watermark_table SET WatermarkValue='{max_date}'
                WHERE TableName = '{table_name}' """
    print(sql_qry)            
    df_watermark_updated=spark.sql(sql_qry)    
    num_updated_rows = df_watermark_updated.first()['num_affected_rows']
    print(f"Watermark updated for {table_name}, watermark rows updated: {num_updated_rows}")
