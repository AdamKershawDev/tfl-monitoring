# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## nb_udfs
# 
# Contains useful UDFs to be used in transform processes

# MARKDOWN ********************

# ### Import libraries

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

# ### Environment Variables

# CELL ********************

%run nb_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_environment_variables():
    """
    Checks whether the currnet workspace is dev, test or prod and returns workspace and lakehouse id's

    Parameters:
    None

    Return:
    bronze_workspace_id 
    bronze_lakehouse_id 
    bronze_lakehouse_path 
    silver_workspace_id 
    silver_lakehouse_id 
    silver_lakehouse_path
    """

    workspace_name = notebookutils.runtime.context.get("currentWorkspaceName")
    environment = workspace_name.split("_")[0] # Get the inital section of the worspace name (dev or prod)
    current_config = config[environment] # Read from the config notebook

    bronze_workspace_id = current_config['bronze_workspace_id']
    bronze_lakehouse_id = current_config['bronze_lakehouse_id']
    bronze_lakehouse_path = f"abfss://{bronze_workspace_id}@onelake.dfs.fabric.microsoft.com/{bronze_lakehouse_id}/"

    silver_workspace_id = current_config['silver_workspace_id']
    silver_lakehouse_id = current_config['silver_lakehouse_id']
    silver_lakehouse_path = f"abfss://{silver_workspace_id}@onelake.dfs.fabric.microsoft.com/{silver_lakehouse_id}/Tables/"

    return bronze_workspace_id, bronze_lakehouse_id, bronze_lakehouse_path, silver_workspace_id, silver_lakehouse_id, silver_lakehouse_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### API Reading

# CELL ********************

def copy_api_json_to_lakehouse(url, file_path):
    """
    Retrieves JSON from the given URL and saves it to the specified file path

    Parameters:
    url: The URL to read from
    file_path: The full file path (including filename) to save the file to

    Return:
    None
    
    """
    response = requests.get(url)
    data = response.json()

    # save temp in notebook
    local_path = "/tmp/file.json"
    with open(local_path, "w") as f:
        json.dump(data, f)

    # save the file
    mssparkutils.fs.cp(f"file:{local_path}", file_path)
    print('File Copied to Lakehouse')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Retrieve timestamps and dates

# CELL ********************

def get_timestamp_parts(process_timestamp):
    """
    - Takes a timestamp and splits it and returns as year, month, day parts
    - Returns timestamp as string suitable for filenames
    - Returns yesterdays date in yyyy-mm-dd format

    Parameters:
    process_timestamp: The timestmap to be transformed

    Return:
    process_timestamp: the original timestamp
    process_timestamp_string: the timestamp in %Y-%m-%d%H%M%S format
    process year: the year of the timestamp
    process_month: the month of the timestamp
    process_day: the day on month of the timestamp including leading zeroes
    date_yesterday: the date 1 day before the timestamp in yyyy-MM-dd format
    
    """
    ts_df = spark.range(1).withColumn("ts", process_timestamp)
    timestamp_value = ts_df.select("ts").collect()[0]["ts"]
    process_timestamp_string = timestamp_value.strftime("%Y-%m-%d%H%M%S")
    process_year = timestamp_value.strftime("%Y")
    process_month = timestamp_value.strftime("%m")
    process_day = timestamp_value.strftime("%d")

    yesterday_df = ts_df.withColumn("yesterday", date_sub(col("ts"), 1)) \
                        .withColumn("yesterday_str", date_format(col("yesterday"), "yyyy-MM-dd"))
    date_yesterday = yesterday_df.select("yesterday_str").collect()[0]["yesterday_str"]
    return(process_timestamp, process_year, process_month, process_day, process_timestamp_string, date_yesterday)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Manage process_files table

# CELL ********************

def write_to_processed_files_table(lakehouse_path,file_type,source_file,processed_to_bronze_timestamp, process_name, end_timestamp):
    """
    Takes parameters and uses them to update the utility_processed_files table

    Parameters:
    lakehouse_path: path to the lakehouse
    file_type: the type of file (for example 'flood_readings')
    source_file: the source file path
    processed_to_bronze_timestamp: the timestamp of the process writing the file to the bronze lakehouse
    process_name: the process calling the function
    end_timestamp: the end timestamp of the file, if applicable

    Return:
    None
    
    """

    data = [Row(file_type = file_type,
                source_file= source_file,
                processed_to_bronze_timestamp= processed_to_bronze_timestamp,
                process_name= process_name,
                end_timestamp= end_timestamp)]

    df_processed = spark.createDataFrame(data)
    df_processed.write.format("delta").mode("append").save(f"{lakehouse_path}/Tables/utility_processed_files")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
