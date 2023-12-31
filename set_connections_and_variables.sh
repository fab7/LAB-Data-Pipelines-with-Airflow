#!/bin/bash
#
# HELPER: Run the follwing command and observe the JSON output: 
#   airflow connections get aws_credentials -o json 

#[{"id": "1", 
# "conn_id": "aws_credentials",
# "conn_type": "aws", 
# "description": "", 
# "host": "", 
# "schema": "", 
# "login": "<ADD-YOUR-KEY-HERE>", 
# "password": "<ADD-YOUR-PASSWORD-HERE>", 
# "port": null, 
# "is_encrypted": "False", 
# "is_extra_encrypted": "False", 
# "extra_dejson": {}, 
# "get_uri": "aws://<ADD-YOUR-KEY-HERE>:<ADD-YOUR-PASSWORD-HERE>"
#}]

# SAVING the URI
airflow connections add aws_credentials --conn-uri 'aws://<ADD-YOUR-KEY-HERE>:<ADD-YOUR-PASSWORD-HERE>'

# HELPER: run the follwing command and observe the JSON output: 
#   airflow connections get redshift -o json

# [{"id": "3", 
# "conn_id": "redshift", 
# "conn_type": "redshift", 
# "description": "", 
# "host": "default.859321506295.us-east-1.redshift-serverless.amazonaws.com", 
# "schema": "dev", 
# "login": "awsuser", 
# "password": "R3dsh1ft", 
# "port": "5439", 
# "is_encrypted": "False", 
# "is_extra_encrypted": "False", 
# "extra_dejson": {}, 
# "get_uri": "redshift://awsuser:R3dsh1ft@default.859321506295.us-east-1.redshift-serverless.amazonaws.com:5439/dev"}]

# SAVING Airflow connection to Redshift
airflow connections add redshift --conn-uri 'redshift://awsuser:R3sh1ft@default.487854660273.us-east-1.redshift-serverless.amazonaws.com:5439/dev'

# SAVING Airflow variable: S3 bucket name
airflow variables set s3_bucket fab-se4s-bucket
#
# SAVING Airflow variable: S3 prfix name
airflow variables set s3_prefix data-pipelines
