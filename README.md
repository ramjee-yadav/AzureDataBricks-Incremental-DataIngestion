# Incremental Data Ingestion pipeline using Azure data bricks

## Overview
In this project a demo of extraction and ingestion for incremental data is given.
![architecture](/ADB-IncrementalIngestion.drawio.png)

## Prerequisite
Below resources are required with relevant access
Azure SQL or any other SQL DB
Azure databricks
Azure Key vault( to store Service principle secretes and sql connection details)

## How to ingest incremental data
I am using watermark based approach to extract the data from SQL DB.
I am using Autoloader to load the new files whichever arrives by the time we trigger it.