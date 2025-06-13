# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## nb_config
# 
# Holds GUIDs for the dev and prod workspace and lakehouse

# CELL ********************

import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

config = {
	"dev": {
		"bronze_workspace_id": "dc11eb45-d910-4a79-97b3-f1dee4e746d2",
		"bronze_workspace_name": "dev_tfl_monitoring_bronze",
		"bronze_lakehouse_id": "ef6076c4-e7b7-40c1-aaed-75a2a1c416f1",
		"bronze_lakehouse_name": "lh_tfl_monitoring_bronze",
		
		"silver_workspace_id": "",
		"silver_workspace_name": "",
		"silver_lakehouse_id": "",
		"silver_lakehouse_name": ""
	},
	"prod": {
		"bronze_workspace_id": "",
		"bronze_workspace_name": "",
		"bronze_lakehouse_id": "",
		"bronze_lakehouse_name": "",
		
		"silver_workspace_id": "",
		"silver_workspace_name": "",
		"silver_lakehouse_id": "",
		"silver_lakehouse_name": ""
	}
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pipeline_config = {
	"dc11eb45-d910-4a79-97b3-f1dee4e746d2": {
		"bronze_workspace_id": "dc11eb45-d910-4a79-97b3-f1dee4e746d2",
		"bronze_workspace_name": "dev_tfl_monitoring_bronze",
		"bronze_lakehouse_id": "ef6076c4-e7b7-40c1-aaed-75a2a1c416f1",
		"bronze_lakehouse_name": "lh_tfl_monitoring_bronze",
		
		"silver_workspace_id": "",
		"silver_workspace_name": "",
		"silver_lakehouse_id": "",
		"silver_lakehouse_name": ""
	},
	"prod": {
		"workspace_id": "",
		"workspace_name": "",
		"lakehouse_id": "",
		"lakehouse_name": ""
	}
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(json.dumps(pipeline_config))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
