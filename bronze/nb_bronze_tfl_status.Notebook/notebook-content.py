# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ef6076c4-e7b7-40c1-aaed-75a2a1c416f1",
# META       "default_lakehouse_name": "lh_tfl_monitoring_bronze",
# META       "default_lakehouse_workspace_id": "dc11eb45-d910-4a79-97b3-f1dee4e746d2",
# META       "known_lakehouses": [
# META         {
# META           "id": "ef6076c4-e7b7-40c1-aaed-75a2a1c416f1"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook: nb_bronze_tfl_lines
# 
# ### Purpose:
# This notebook performs the ingestion of line status data from the TFL Unified API into the bronze lakehouse. The JSON response from the API is landed into the **Files/tfl_monitoring/status**. 
# 
# ### Process:
# 1. Generate URL for API Call
# 2. Copy JSON to Lakehouse
#  
# ### Change log:
#  
#  | Date       | Author      | Change Description                                          |
#  |:-----------|:-----------|:------------------------------------------------------------|
#  | 2025-06-13 | Adam Kershaw    | Initial notebook creation |

# CELL ********************

import requests
import json
import os
from pyspark.sql.functions import explode, col, lit, input_file_name, regexp_extract, to_timestamp, current_timestamp, year, month, dayofmonth, lpad, date_format, date_sub
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType
from pyspark.sql import DataFrame, Row
import sempy.fabric as fabric
from delta.tables import DeltaTable


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### <mark>Set Manual Variables</mark>

# CELL ********************

file_type = 'status'
relative_path = f"Files/bronze/tfl_monitoring/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Run Support Notebooks

# CELL ********************

%run nb_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_udfs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Retrieve Environment Variables

# CELL ********************

# Map environment variables based on workspace name
bronze_workspace_id, bronze_lakehouse_id, bronze_lakehouse_path, silver_workspace_id, silver_lakehouse_id, silver_lakehouse_path = get_environment_variables()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Derive Common Variables

# CELL ********************

notebook_name = notebookutils.runtime.context['currentNotebookName']
bronze_directory = f"{bronze_lakehouse_path}{relative_path}{file_type}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Get Timestamps

# CELL ********************

#Get current timestamp and extract parts to use in filepaths
process_timestamp, process_year, process_month, process_day, process_timestamp_string, date_yesterday = get_timestamp_parts(current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Set target path for bronze file 
bronze_file_path = (
    f"{bronze_directory}/ingestion_year={process_year}/ingestion_month={process_month}/ingestion_day={process_day}/{file_type}_{process_timestamp_string}.json"
)
print(bronze_file_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Generate URL for API

# CELL ********************

# Generate the URL for API
api_key = mssparkutils.credentials.getSecret('https://kv-fabric-uksouth.vault.azure.net/','tfl-api-key-dev')

url = f"https://api.tfl.gov.uk/Line/Mode/tube/Status?app_key={api_key}"
print(url)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Copy JSON from API to Lakehouse

# CELL ********************

copy_api_json_to_lakehouse(url, bronze_file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Update processed files table

# CELL ********************

write_to_processed_files_table(bronze_lakehouse_path,file_type,bronze_file_path,process_timestamp_string, notebook_name, date_yesterday)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
