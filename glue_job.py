from state_tracker import *


class GlueShellJob:
    def __init__(self, job_name, environment_configuration_path, job_start='1900-01-01 00:00:00',
                 job_end='1900-01-01 00:00:00'):
        log("initializing GlueJob class for {0}".format(job_name))
        self.job_name = job_name

        # set clients
        self.glue_client = boto3.client('glue', region_name="us-east-1")
        self.s3client = boto3.client('s3', region_name="us-east-1")
        self.sns_client = boto3.client('sns', region_name="us-east-1")

        # set configuration from config files
        self.env_config = parse_config_from_s3(environment_configuration_path)
        self.job_config = get_job_config(job_name, self.env_config)

        # determine job progress tracking method
        self.tracking_type_id = self.job_config["TrackingTypeID"]
        self.tracked_object = ""
        if self.tracking_type_id == 1:
            self.tracked_object = self.job_config["Table"]["Name"]
        elif self.tracking_type_id == 2:
            self.tracked_object = self.job_config["StreamName"]
        elif self.tracking_type_id == 3:
            self.tracked_object = self.job_name

        # set exception handling
        if 'HighPriority' in self.job_config:
            priority_flag = self.job_config["HighPriority"]
        else:
            priority_flag = False
        if priority_flag:
            self.failure_notification = self.env_config["L360_HighPriorityFailureNotification"]
        else:
            self.failure_notification = self.env_config["L360_FailureNotification"]
        self.low_priority_failure_notification = self.env_config["L360_FailureNotification"]
        self.exception_logger = logging.getLogger()

        # check for debug
        self.debug = False
        if 'DebugMode' in self.job_config:
            self.debug = self.job_config["DebugMode"]

        # initialize state tracker
        self.statetracker = StateTracker(self.env_config)
        self.job_history_id = self.statetracker.track_job_start(self.job_name, self.tracking_type_id)
        self.process_increment = self.job_config["ProcessIncrement"]
        self.use_custom_date_override = False
        # get max process date
        max_process_dt_results = self.statetracker.get_max_process_dt(self.job_config)
        self.max_process_dt = max_process_dt_results["max_process_value"]
        max_process_dt_override = datetime.strptime(job_end, '%Y-%m-%d %H:%M:%S')
        if job_end != '1900-01-01 00:00:00' and max_process_dt_override < self.max_process_dt:
            self.max_process_dt = max_process_dt_override
            log("max process date set to {0} due to parameter override".format(self.max_process_dt))

        self.missing_dependency = max_process_dt_results["missing_dependency"]
        # get current processing date
        self.current_process_dt = self.statetracker.get_current_process_dt(self.job_name, self.tracked_object,
                                                                           self.process_increment, self.job_config)
        current_process_dt_override = datetime.strptime(job_start, '%Y-%m-%d %H:%M:%S')
        if job_start != '1900-01-01 00:00:00':
            self.current_process_dt = current_process_dt_override
            self.use_custom_date_override = True
            log("current process date set to {0} due to parameter override".format(self.current_process_dt))

        self.job_stop_flag = False
        log('starting process dt = {0}, max process dt = {1}'.format(self.current_process_dt, self.max_process_dt))

        # alert if dependency is missing
        if not self.debug:
            if self.missing_dependency is not None:
                notification_msg = "Job {0} changed max process date due to missing dependency from Job {1}".format(
                    self.job_name, self.missing_dependency)
                response = self.sns_client.publish(
                    TopicArn=self.low_priority_failure_notification,
                    Message=notification_msg
                )
                log(notification_msg)
                log(response)

        # set workspace and initialize rowcounter
        self.workspace_bucket = self.env_config["L360_BucketGlueWorkspace"]
        self.rowcount = None

    def run_job(self):
        """
        Loops through increments (job config "ProcessIncrement" value) of the job until the max process date is reached.
        Each increment calls job_process() one time and tracks progress in the state tracker.
        Then the method updates the job's state from the state tracker before ending.
        """
        try:
            while self.current_process_dt <= self.max_process_dt and not self.job_stop_flag:
                log("processing {0}".format(self.current_process_dt))

                # reset row count every loop
                self.rowcount = None
                # run main job loop
                self.job_process()
                # track completion and get next value to process
                self.job_stop_flag = self.statetracker.track_job_progress(self.job_history_id, self.current_process_dt,
                                                                          trackedobject=self.tracked_object,
                                                                          rowcount=self.rowcount)
                # support for optional date parameter overrides
                if self.use_custom_date_override:
                    if self.process_increment == "hour":
                        self.current_process_dt = self.current_process_dt + timedelta(hours=1)
                    if self.process_increment == "day":
                        self.current_process_dt = self.current_process_dt + timedelta(days=1)
                    if self.process_increment == "month":
                        self.current_process_dt = self.statetracker.get_next_month(self.current_process_dt)
                else:
                    self.current_process_dt = self.statetracker.get_current_process_dt(self.job_name,
                                                                                       self.tracked_object,
                                                                                       self.process_increment)
        # END JOB
        except Exception as e:
            self.exception_logger.exception("fatal error in main loop")
            log("job failed, please review logs")
            if not self.debug:
                notification_msg = "Job {0} failed with error: {1}".format(self.job_name, e)
                self.sns_client.publish(
                    TopicArn=self.failure_notification,
                    Message=notification_msg,
                )
            logged_error = str(e)[:255]
            self.statetracker.track_job_end(self.job_history_id, False, error_message=logged_error)
            raise

        self.statetracker.track_job_end(self.job_history_id, True)
        log("job completed successfully")

    def job_process(self):
        """
        overwrite this method with the specific job logic. This method will be called once per increment,
        as defined in it's job config "ProcessIncrement" value.
        """
        log("this is the main job loop; override this with job-specific logic")


log("glue_job library loaded")
