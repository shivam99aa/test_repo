#!/usr/bin/env bash

aws s3 cp ./state_tracker.py s3://life360-glue-libraries-prod --profile 102611674515_DataEngineer
#aws s3 cp ./catalog_manager.py s3://life360-glue-libraries-prod --profile prod
#aws s3 cp ./glue_job.py s3://life360-glue-libraries-prod --profile prod
#aws s3 cp ./eggs/PyMySQL-0.9.3-py2.7.egg s3://life360-glue-libraries-prod --profile prod
#aws s3 cp ./eggs/snowflake_connector_python-1.9.0-py2.7.egg s3://life360-glue-libraries-prod --profile prod
#aws s3 cp ./jars/snowflake-jdbc-3.6.27.jar s3://life360-glue-libraries-prod --profile prod
#aws s3 cp ./jars/spark-snowflake_2.11-2.4.13-spark_2.4.jar s3://life360-glue-libraries-prod --profile prod
#aws s3 cp ./job_deployment.py s3://life360-glue-libraries-prod --profile prod
aws s3 cp ./job_deployment_config.json s3://life360-glue-libraries-prod --profile prod
#aws s3 cp ./job_deployment_pythonshell_config.json s3://life360-glue-libraries-prod --profile prod
