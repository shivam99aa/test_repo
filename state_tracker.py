import sys
from datetime import datetime, timedelta
import time
import pymysql
import boto3
import json
import logging

# workaround for dev endpoint version incompatibility
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse


# generic logger
def log(log_string):
    """
    logger function that doesn't require a state tracker object.
    Args:
        log_string (string): log to print

    """
    log_dt = datetime.now()
    logval = "{0} Life360LogEntry - {1}".format(log_dt, log_string)
    print(logval)
    sys.stdout.flush()
    return


def parse_config_from_s3(config_filepath, env_config=None):
    """
    parses a job config and adds environment specific values

    Args:
        config_filepath (string): s3 path to job config file
        env_config (dict): json object that contains env specific data

    Returns:
        dict: same structure as config file but with env specific data
    """
    config_loc = urlparse(config_filepath)
    s3 = boto3.resource('s3')
    config_bucket = config_loc.netloc
    config_key = config_loc.path.lstrip('/')
    config_object = s3.Object(config_bucket, config_key)
    file_content = str(config_object.get()['Body'].read().decode('utf-8'))
    # optionally parse the config for env vars and replace them with values
    if env_config:
        for key, value in env_config.items():
            if "L360" in key:
                file_content = str(file_content.replace("${0}".format(key), value))
    json_config = json.loads(file_content)
    return json_config


# get config from glue catalog rather than json file
def get_config_from_catalog(database_name, table_name, glue_client):
    """
    returns the glue catalog data for a table

    Args:
        database_name (string): db of table
        table_name (string): name of table
        glue_client (client): glue client from job

    Returns:
        dict: glue catalog data including schema and locations; looks similar to job config layout
    """
    response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
    del response["Table"]["DatabaseName"]
    response["DatabaseName"] = database_name
    return response


def get_job_config(job_name, env_config):
    """
    reads a job config from s3

    Args:
        job_name (string): name of job
        env_config (dict): env_config.json data

    Returns:
        dict: job config with env specific data
    """
    config_path = '{0}/{1}/job_config.json'.format(env_config['L360_BucketGlueJob'], job_name)
    job_config = parse_config_from_s3(config_path, env_config)
    return job_config


# deprecated; use get_max_process_dt()
def get_max_process_value(increment, delay=1):
    """
    get the max date that a job can process. Usually this is equal to the last whole day or hour, depending on daily or
    hourly processing.

    Args:
        increment (string): hour or day
        delay (int): hours or days of additional delay

    Returns:
        datetime: max date that the job can process
    """
    # default to daily processing
    max_process_value = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    if increment == 'hour':
        max_process_value = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(hours=delay)
    return max_process_value


# STATETRACKER
class StateTracker:
    def __init__(self, config):
        self.trackerhost = config["StateTrackerAuth"]["stHost"]
        self.user = config["StateTrackerAuth"]["user"]
        self.passwd = config["StateTrackerAuth"]["passwd"]
        self.db = config["StateTrackerAuth"]["db"]
        self.connect_timeout = config["StateTrackerAuth"]["connect_timeout"]

        # initialize logger
        msg_format = '%(asctime)s %(name)s %(levelname)s - %(message)s'
        dt_format = '%Y-%m-%d %H:%M:%S,%f'
        logging.basicConfig(format=msg_format, datefmt=dt_format)
        self.logger = logging.getLogger('Life360LogEntry')

    # deprecated, use non-class log function above
    def log(self, log_string):
        """
        logger for jobs that adds timestamp, writes to cloudwatch logs, and manages multithreaded jobs

        Args:
            log_string (string): message to log
        """
        self.logger.warning(log_string)
        sys.stdout.flush()
        return

    def establish_connection(self):
        """
        establishes a db connection to the state tracker db

        Returns:
            mysql connection
        """
        retry_limit = 10
        i = 0
        while i < retry_limit:
            i += 1
            try:
                cxn = pymysql.connect(
                    self.trackerhost,
                    user=self.user,
                    passwd=self.passwd,
                    db=self.db,
                    connect_timeout=self.connect_timeout)

                return cxn
            except pymysql.err.OperationalError as e:
                if i == retry_limit:
                    log('unable to establish connection to state tracker after {0} attempts; aborting...'.format(i))
                    log(str(e))
                    raise
                else:
                    log('unable to establish connection to state tracker; retrying after {0} seconds...'.format(i))
                    log(str(e))
                    time.sleep(i)
        return

    def track_job_start(self, jobname, trackedtypeid=1, processincrement="day"):
        """
            called once every time a spark job is kicked off

        Args:
            jobname (string): name of job
            trackedtypeid (int): 1=Partition (use for table processing), 2=Stream (stream processing), 3=Increment
            (used when a job doesn't have any other objects to track, such as a cleanup job)

        Returns:
            int: job history id that can be used by job to keep track of objects or data that it affects. Is also used
            to track job progress.
        """
        params = [jobname, trackedtypeid]
        with self.establish_connection() as conn:
            conn.callproc("track_job_start", params)
            result = conn.fetchone()[0]
        return result

    # updates job instance status via job ID
    def track_job_end(self, jobhistoryid, resultflag, error_message=None):
        """
            called once every time a job ends

        Args:
            jobhistoryid (int): history id from track job start call
            resultflag (bool): 1=Success, 2=Failure
            error_message (string): optional error message (only passed by failed jobs)

        Returns:
            none
        """
        params = [jobhistoryid, resultflag, error_message]
        with self.establish_connection() as conn:
            conn.callproc("track_job_end", params)
            conn.close()
        return

    def track_job_progress(self, jobhistoryid, incrementdt, trackedobject='', trackedsubobject='', trackedid='',
                           rowcount=None):
        """
            called once for every partition or increment that a job processes. Can be used as a checkpoint for jobs.
            This is also how the state tracker db informs a job that it needs to end (due to a stop_job call).

        Args:
            jobhistoryid (int): history id from track job start call
            incrementdt (datetime): date increment that was processed (e.g. '2020-03-01' to record a job that processed
            data for 2020-03-01)
            trackedobject (string): table, stream, or job name (tracking types 1-3) of the object being tracked
            trackedsubobject (string): for tracking a shard within a stream
            trackedid (string): sequence number of stream for stream tracking
            rowcount (bigint): optional row count for partition (only for table jobs)

        Returns:
            bool: stop_job flag. If this is set to TRUE, then the job needs to end without further processing. Used to
            gracefully end long running jobs.
        """
        params = [jobhistoryid, incrementdt, trackedobject, trackedsubobject, trackedid, rowcount]
        with self.establish_connection() as conn:
            conn.callproc("track_job_progress", params)
            row = conn.fetchone()
            if row is not None:
                result = row[0]
            conn.close()
        return result

    # call to get last date value processed by any job_type
    def get_last_processed_datetime_for_job(self, jobname, tracked_object):
        """
            call to get last date value processed by any job_type

        Args:
            jobname (string): name of job
            tracked_object (string) table, stream, or job name (for tracking type 3) of the object being tracked

        Returns:
            datetime: last datetime that was successfully processed by job for the given object
        """
        params = [jobname, tracked_object]
        result = None
        with self.establish_connection() as conn:
            conn.callproc("get_last_processed_datetime_for_job", params)
            row = conn.fetchone()
            if row is not None:
                result = row[0]
            conn.close()
        return result

    # deprecated
    def get_next_batch_id(self, tablename):
        """
        use this with a call to monotonically_increasing_id() to generate a unique composite key

        Args:
            tablename (string): name of table

        Returns:
            bigint: returns a unique batch_id
        """
        params = [tablename]
        with self.establish_connection() as conn:
            conn.callproc("get_batch_id", params)
            result = conn.fetchone()[0]
            if result is None:
                final = None
            else:
                final = result
            conn.close()
        return final

    def get_last_sequence_number_processed(self, jobname, streamname, shardid):
        """
        used for jobs to determine where to start processing a shard in a given stream

        Args:
            jobname (string): name of job
            streamname (string): name of stream
            shardid (string): id of specific shard in the stream that is being checked

        Returns:
            string: returns the last sequence number that was processed in the stream
        """
        params = [jobname, streamname, shardid]
        result = ''
        with self.establish_connection() as conn:
            conn.callproc("get_last_processed_stream_sequence_number", params)
            row = conn.fetchone()
            if row is not None:
                result = row[0]
            conn.close()
        return result

    def get_next_datetime_value_to_process(self, jobname, trackedobject='', increment='day'):
        """
        used for jobs to determine the next batch to process

        Args:
            jobname (string): name of job
            trackedobject (string): name of table or other object being processed
            increment (string): hour, day, or month

        Returns:
            datetime: returns the last datetime increment that was processed by job.
        """
        result = None
        params = [jobname, trackedobject]

        with self.establish_connection() as conn:
            conn.callproc("get_last_processed_datetime_for_job", params)
            sproc_result = conn.fetchone()[0]
            conn.close()

        if sproc_result is not None:
            if increment == "hour":
                result = sproc_result + timedelta(hours=1)
            if increment == "day":
                result = sproc_result + timedelta(days=1)
            if increment == "month":
                result = self.get_next_month(sproc_result)
        return result

    def check_dependencies(self, dependency_list):
        """
        used by jobs to make sure upstream processes have completed

        Args:
            dependency_list (list): key = job to check, value = tracked object within that job to check (e.g., table
            name or stream name)

        Returns:
            dict: returns the latest datetime increment that has been successfully processed by all jobs in the
            dependency list.
        """
        dependency_date = None
        dependent_job = None
        for dependency, trackedobject in dependency_list:
            last_processed_dependency_dt = self.get_last_processed_datetime_for_job(dependency, trackedobject)
            if last_processed_dependency_dt is not None:
                if dependency_date is None:
                    dependency_date = last_processed_dependency_dt
                    dependent_job = dependency
                elif last_processed_dependency_dt < dependency_date:
                    dependency_date = last_processed_dependency_dt
                    dependent_job = dependency
        return_val = {"dependency_date": dependency_date, "dependent_job": dependent_job}
        return return_val

    def track_active_shards(self, jobhistoryid, shard_id, active):
        """
        log which shards are active on each job run. This handles scenarios where a stream is scaled and some shards
        are no longer receiving new data.

        Args:
            jobhistoryid (int): job history id of job instance making the change
            shard_id (string): id of specific shard in the stream that is being checked
            active (boolean): active flag for shard
        """
        params = [jobhistoryid, shard_id, active]
        with self.establish_connection() as conn:
            conn.callproc("track_active_shard", params)
            conn.close()
        return

    def track_crawler_tables(self, crawler, tracked_table):
        """
        add a table to the tracked table list for a crawler.

        Args:
            crawler (string): crawler name
            tracked_table (string): name of table that the crawler is tracking

        Returns:
            boolean: returns a flag to indicate whether or not the crawler should manage the table. Useful for cases
            where the crawler can't generate a usable schema.
        """
        params = [crawler, tracked_table]
        with self.establish_connection() as conn:
            conn.callproc("track_crawler_table", params)
            result = conn.fetchone()[0]
            conn.close()
        return result

    def get_current_process_dt(self, job_name, tracked_object_name, process_increment, conf=None):
        """
        get start datetime for job

        Args:
            job_name (string): name of job
            tracked_object_name (string): name of table
            process_increment (string): day or hour
            conf (dict): config file for job

        Returns:
            datetime: returns the next datetime increment that needs to be processed by the job
        """
        current_process_dt = self.get_next_datetime_value_to_process(job_name, trackedobject=tracked_object_name,
                                                                     increment=process_increment)
        if not current_process_dt and conf is not None:
            current_process_dt = datetime.strptime(conf['StartingPartition'], '%Y-%m-%d %H:%M:%S')
        return current_process_dt

    # new function that includes dependencies
    def get_max_process_dt(self, job_conf):
        """
        get max end datetime for job

        Args:
            job_conf (dict): config file for job

        Returns:
            dict: key = table name, value = start datetime
        """
        # get vars from config
        increment = job_conf["ProcessIncrement"]
        depend_list = job_conf["JobDependencies"]
        failed_dependency = None
        if 'JobDelay' in job_conf:
            delay = job_conf["JobDelay"]
        else:
            delay = 1
        # determine max value as of current date
        max_process_value = datetime.utcnow()
        if increment == "hour":
            max_process_value = max_process_value.replace(minute=0, second=0, microsecond=0) - timedelta(hours=delay)
        if increment == "day":
            max_process_value = max_process_value.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
                days=delay)
        if increment == "month":
            max_process_value = self.get_previous_month(max_process_value)

        # check dependencies and adjust max value if a dependency is behind
        if depend_list:
            dependency_results = self.check_dependencies(depend_list)
            last_processed_dt = dependency_results["dependency_date"]
            if last_processed_dt is None:
                last_processed_dt = max_process_value - timedelta(days=1)
            dependent_job = dependency_results["dependent_job"]
            if last_processed_dt < max_process_value:
                self.log("updating max_process_dt due to missing dependency, was {0} now {1}".format(max_process_value,
                                                                                                     last_processed_dt))
                max_process_value = last_processed_dt
                failed_dependency = dependent_job
        return_val = {"max_process_value": max_process_value, "missing_dependency": failed_dependency}
        return return_val

    def get_crawler_list(self, crawler_id, return_all=False):
        """
        returns a list of tables to be crawled

        Args:
            crawler_id (int): id of crawler to run
            return_all (bool): if true then return every table,
                if false only return tables that the crawler is allowed to manage

        Returns:
            list: returns a list of tables/objects for the crawler to crawl
        """
        params = [crawler_id, return_all]
        with self.establish_connection() as conn:
            conn.callproc("get_crawler_tables", params)
            result = conn.fetchall()
            conn.close()

        table_list = []
        for row in result:
            table_list.append(row[0])

        return table_list

    def sync_crawler_table_list(self, crawler, incoming_table_list):
        """
        compares the inputted list of tables to what is currently stored in the state tracker and adds any tables that
        are missing

        Args:
            crawler (string): crawler name
            incoming_table_list (list): list of table names

        Returns:
            int: count of tables added
        """
        existing_table_list = []
        crawler_id = None
        with self.establish_connection() as conn:
            # get crawler id
            params = [crawler]
            conn.callproc("lookup_crawler_id", params)
            result = conn.fetchone()
            if result is not None:
                crawler_id = result[0]
            # get existing tables for crawler
            if crawler_id is not None:
                params = [crawler_id, True]
                conn.callproc("get_crawler_tables", params)
                results = conn.fetchall()
                for row in results:
                    existing_table_list.append(row[0])
            conn.close()

        # insert new tables
        new_table_list = [x for x in incoming_table_list if x not in existing_table_list]
        if len(new_table_list) > 0:
            with self.establish_connection() as conn:
                for new_table in new_table_list:
                    params = [crawler, new_table]
                    conn.callproc("track_crawler_table", params)
                conn.close()
        return

    def get_job_status_for_models(self):
        """
        Returns:
            list: returns a list of tables and their last date of processing for all active model jobs
        """
        with self.establish_connection() as conn:
            conn.callproc("get_job_status_for_models")
            result = conn.fetchall()
            conn.close()

        table_list = []
        for row in result:
            table_list.append(row)

        return table_list

    @staticmethod
    def get_next_month(current_datetime):
        """
        returns the first day of the next month

        Args:
            current_datetime (datetime): current datetime

        Returns:
            datetime: the first day of the next month
        """
        result = current_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
        current_month = current_datetime.month
        next_month = current_month
        while next_month == current_month:
            result = result + timedelta(days=1)
            next_month = result.month
        return result

    @staticmethod
    def get_previous_month(current_datetime):
        """
        returns the first day of the previous month

        Args:
            current_datetime (datetime): current datetime

        Returns:
            datetime: the first day of the previous month
        """
        result = current_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
        current_month = current_datetime.month
        next_month = current_month
        while next_month == current_month:
            result = result - timedelta(days=1)
            next_month = result.month
        result = result.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return result
