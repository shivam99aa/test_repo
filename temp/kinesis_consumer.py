from datetime import datetime, timedelta
import time


def log(log_string):
    log_dt = datetime.now()
    logval = "{0} Life360LogEntry - {1}".format(log_dt, log_string)
    print(logval)
    return


class KinesisConsumer:
    def __init__(self, stream, kinesis_client):
        log("initializing KinesisConsumer for stream {0}".format(stream))
        self.stream_name = stream
        self.client = kinesis_client
        self.shard_list = self.client.list_shards(StreamName=self.stream_name)["Shards"]


class ShardProcessor:
    def __init__(self, stream, kinesis_client, shardid, startdate, seqnum, jobhistoryid, process_increment_limit,
                 consumerarn=None):
        log("initializing ShardProcessor for stream {0}, shard_id {1}".format(stream, shardid))
        self.stream_name = stream
        self.client = kinesis_client
        self.shard_id = shardid
        self.shard_iterator = None
        self.limiter = process_increment_limit
        self.shard_iterator_type = 'AFTER_SEQUENCE_NUMBER'
        self.sequence_number = str(seqnum)
        self.approx_arrival_date = startdate
        self.latency = 1
        self.job_history_id = jobhistoryid
        self.time_of_last_read = datetime.now() + timedelta(seconds=-5)
        self.time_of_last_write = datetime.now()
        self.consumer_arn = consumerarn

    def determine_shard_iterator(self, iterator_type=None, iterator_time=None):
        if iterator_time:
            self.approx_arrival_date = iterator_time

        if iterator_type:
            self.shard_iterator_type = iterator_type
        else:
            if not self.sequence_number:
                self.shard_iterator_type = 'AT_TIMESTAMP'
            else:
                self.shard_iterator_type = 'AFTER_SEQUENCE_NUMBER'

        if self.shard_iterator_type == 'AT_TIMESTAMP':
            self.sequence_number = '0'

        # set shard iterator
        shard_iterator_response = self.client.get_shard_iterator(
            StreamName=self.stream_name,
            ShardId=self.shard_id,
            ShardIteratorType=self.shard_iterator_type,
            Timestamp=self.approx_arrival_date,
            StartingSequenceNumber=self.sequence_number)
        self.shard_iterator = shard_iterator_response["ShardIterator"]
        return True

    def get_records(self):
        if (datetime.now() - self.time_of_last_read).total_seconds() < 1:
            time.sleep(1)
        record_list = self.client.get_records(
            ShardIterator=self.shard_iterator,
            Limit=self.limiter
        )

        if 'NextShardIterator' in record_list:
            self.shard_iterator = record_list["NextShardIterator"]
        else:
            self.shard_iterator = None
        self.latency = record_list['MillisBehindLatest']
        self.time_of_last_read = datetime.now()
        return record_list

    def determine_shard_iterator_by_subscription(self):
        if not self.sequence_number:
            self.shard_iterator_type = 'AT_TIMESTAMP'
            self.sequence_number = "0"
        else:
            self.shard_iterator_type = 'AFTER_SEQUENCE_NUMBER'

    def get_records_by_subscription(self):
        time_to_sleep = (datetime.now() - self.time_of_last_read).total_seconds()
        if time_to_sleep < 1:
            time.sleep(time_to_sleep)
        if self.shard_iterator_type == 'AT_TIMESTAMP':
            self.sequence_number = "0"
        response = self.client.subscribe_to_shard(
            ConsumerARN=self.consumer_arn,
            ShardId=self.shard_id,
            StartingPosition={
                'Type': self.shard_iterator_type, 'SequenceNumber': self.sequence_number, 'Timestamp': self.approx_arrival_date
            }
        )
        event_stream = response["EventStream"]
        if isinstance(event_stream, dict):
            log("get records call returned - {0}".format(str(event_stream)))
            event_stream = None
        return event_stream

    def process_records(self, record_list):
        log("process_records needs to be overridden with job-specific logic")
