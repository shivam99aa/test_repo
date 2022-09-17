from botocore.exceptions import ClientError
from state_tracker import *
import boto3


class Deployment:
    def __init__(self, jobname, environment='dev'):
        log("initializing Deployment for {0}".format(jobname))
        self.s3 = boto3.resource('s3')
        self.cfclient = boto3.client('cloudformation', region_name="us-east-1")
        self.job_name = jobname
        self.stack_name = 'GlueTest6-Job-' + self.job_name

        # get job config
        with open('../{0}/job_config.json'.format(self.job_name), 'r') as job_config:
            self.json_config = json.load(job_config)

        # get environment config
        if environment == 'dev':
            self.env_config_path = 's3://life360-glue-configs-dev/env_config.json'
        else:
            self.env_config_path = 's3://life360-glue-configs-prod/env_config.json'
        self.env_config = parse_config_from_s3(self.env_config_path)

        # get variables from config
        self.job_description = self.json_config['JobDescription']
        self.job_capacity = self.json_config['JobCapacity']
        self.trigger_schedule = self.json_config['TriggerSchedule']
        self.job_bucket = self.env_config['L360_BucketGlueJob']
        self.notification_alerts_email = self.env_config['NotificationAlertsEmail']
        self.library_bucket = self.env_config['L360_BucketGlueLibrary']
        self.library_files = self.env_config['LibraryFiles']
        self.jar_files = self.env_config['JarFiles']
        self.job_role = self.env_config['GlueJobRole']
        self.job_type = self.json_config['JobType']

        # determine remaining parameters
        self.library_bucket_name = self.library_bucket[5:]
        if self.job_type == "pythonshell":
            self.glue_version = "1.0"
            self.deploy_config_url = 'https://{0}.s3.amazonaws.com/job_deployment_pythonshell_config.json'.format(self.library_bucket_name)
        else:
            self.glue_version = "2.0"
            self.deploy_config_url = 'https://{0}.s3.amazonaws.com/job_deployment_config.json'.format(self.library_bucket_name)

        self.job_bucket_name = self.job_bucket[5:]
        self.job_script = '{0}/job_script.py'.format(self.job_name)
        self.job_config = '{0}/job_config.json'.format(self.job_name)
        self.glue_job_temp_dir = '{0}/temp'.format(self.job_bucket)
        self.extra_py_files = self.library_bucket + "/" + ",{0}/".format(self.library_bucket).join(self.library_files)
        self.extra_jar_files = self.library_bucket + "/jars/" + ",{0}/jars/".format(self.library_bucket).join(self.jar_files)
        self.job_script_path = '{0}/{1}'.format(self.job_bucket, self.job_script)
        self.trigger_name = 'Glue-Job-Test5-{0}'.format(self.job_name)

        self.parameter_list = [
            {'ParameterKey': 'JobName', 'ParameterValue': self.job_name+"Test7"},
            {'ParameterKey': 'TempDir', 'ParameterValue': self.glue_job_temp_dir},
            {'ParameterKey': 'PyFiles', 'ParameterValue': self.extra_py_files},
            {'ParameterKey': 'JarFiles', 'ParameterValue': self.extra_jar_files},
            {'ParameterKey': 'EnvConfig', 'ParameterValue': self.env_config_path},
            {'ParameterKey': 'JobDescription', 'ParameterValue': self.job_description},
            {'ParameterKey': 'JobScriptPath', 'ParameterValue': self.job_script_path},
            {'ParameterKey': 'JobType', 'ParameterValue': self.job_type},
            {'ParameterKey': 'GlueVersion', 'ParameterValue': self.glue_version},
            {'ParameterKey': 'JobCapacity', 'ParameterValue': self.job_capacity},
            {'ParameterKey': 'JobRole', 'ParameterValue': self.job_role},
            {'ParameterKey': 'TriggerName', 'ParameterValue': self.trigger_name},
            {'ParameterKey': 'TriggerSchedule', 'ParameterValue': self.trigger_schedule}
        ]

    def upload_files(self):
        """
        uploads job script and config files in the local directory for this job to the S3 Job bucket
        """
        log("deploying files for job {0} to bucket {1}".format(self.job_name, self.job_bucket))
        extra_args = {
            'ACL': 'bucket-owner-full-control'
        }
        self.s3.meta.client.upload_file('./job_config.json', self.job_bucket_name, self.job_config, extra_args)
        self.s3.meta.client.upload_file('./job_script.py', self.job_bucket_name, self.job_script, extra_args)

    def deploy_stack(self):
        """
        Calls Cloudformation template to create or update a deployment stack consisting of this Glue Job and an
        associated trigger

        Returns:
            response to create or update call
        """

        response = {}
        # check if stack exists
        try:
            self.cfclient.describe_stacks(StackName=self.stack_name)
        except ClientError:
            log('stack {0} not found, creating...'.format(self.stack_name))
            stack_exists = False
        else:
            log('stack {0} found, checking for updates...'.format(self.stack_name))
            stack_exists = True
        # create/update stack
        if stack_exists:
            try:
                response = self.cfclient.update_stack(
                    StackName=self.stack_name,
                    TemplateURL=self.deploy_config_url,
                    Capabilities=['CAPABILITY_NAMED_IAM'],
                    Parameters=self.parameter_list)

                log(response)
            except ClientError:
                log('no updates detected in update_stack call')
        else:
            response = self.cfclient.create_stack(
                StackName=self.stack_name,
                TemplateURL=self.deploy_config_url,
                Capabilities=['CAPABILITY_NAMED_IAM'],
                Parameters=self.parameter_list)
            log(response)

        return response
