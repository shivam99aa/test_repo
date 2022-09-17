#import sys
#import os
#from pathlib import Path
#path = Path(os.getcwd())
#while path.parts[-1] != "data-engineering":
#    path = path.parent
#sys.path.append(str(path/"libraries/"))


from job_deployment import *

# boto3.setup_default_session(profile_name='264243034530_DataEngineer')

# get job config locally and retrieve job name
with open('./job_config.json', 'r') as job_config:
    json_config = json.load(job_config)

job_name = json_config['JobName']
job_type = json_config['JobType']

# deploy cloudformation stack
deploy = Deployment(job_name, environment='dev')
deploy.upload_files()
if job_type != 'ec2':
    deploy.deploy_stack()
