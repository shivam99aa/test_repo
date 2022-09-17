import re
import uuid
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number
from awsglue.transforms import Relationalize, ResolveChoice
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from glue_job import *
from catalog_manager import *

##################
# DEBUG SETTINGS #
##################
# import pydevd
# pydevd.settrace('localhost', port=9001, stdoutToServer=True, stderrToServer=True)
# ssh -i /Users/cmcmullen/.ssh/gluedev_endpoint glue@ec2-52-21-28-2.compute-1.amazonaws.com -t gluepython3 /home/glue/scripts/jobs/EventProcessing/Publish-Events/Publish-Events-Finance/job_script.py --env_config_path s3://life360-glue-configs-dev/env_config.json --job_name Publish-Events-Finance --job_start_date '2020-07-01\ 00:00:00' --job_end_date '2020-07-01\ 00:00:00'
# ssh -i /Users/cmcmullen/.ssh/gluedev_endpoint -nNT -g -R :9001:localhost:9001 glue@ec2-52-21-28-2.compute-1.amazonaws.com


def resolve_choices_in_structs(dy, struct):
    for column in dy.schema().fields:
        if column.name == struct:
            if hasattr(column.dataType, "field_map"):
                ep = column.dataType.field_map
                for dict_entry in ep:
                    ep_field = ep[dict_entry]
                    ep_field_name = ep_field.name
                    ep_field_datatype = ep_field.dataType
                    ep_full_name = "event_properties.{0}".format(ep_field_name)
                    if hasattr(ep_field_datatype, "choices"):
                        choices = ep_field_datatype.choices
                        if "double" in choices:
                            cast_to = "cast:double"
                        elif "long" in choices:
                            cast_to = "cast:long"
                        elif "bigint" in choices:
                            cast_to = "cast:bigint"
                        elif "int" in choices:
                            cast_to = "cast:int"
                        elif "boolean" in choices:
                            cast_to = "cast:boolean"
                        else:
                            cast_to = "cast:string"
                        dy = ResolveChoice.apply(dy, specs=[(ep_full_name, cast_to)])
    return dy


def create_catalog_schema_from_dataframe(df, tablename, s3_location, partitionkeys):
    """
    returns a glue catalog table schema based on the inputted dataframe.
    Any column ending in "timestamp" will be given a datatype of "timestamp".

    :param df: dataframe to build a table from
    :param tablename: name to give to the new table
    :param s3_location: where to store the new table
    :param partitionkeys: how to partition the new table
    :return: returns dict representation of schema (compatible with boto3 catalog calls)
    """
    partition_list = []
    for column in partitionkeys:
        partition_list.append(column["Name"])
    # build column list
    column_definition_list = []
    for column in df.dtypes:
        if column[0] not in partition_list:
            name = column[0]
            datatype = column[1]
            # check for nulls
            if datatype == "null":
                datatype = "string"
            # check for timestamp columns
            if name[-10:] == "_timestamp":
                datatype = "timestamp"
            column_dict = {"Name": name, "Type": datatype}
            column_definition_list.append(column_dict)
    # create table schema in parquet format for column list
    schema_dict = {
        "Table": {
            "Name": tablename,
            "TableType": "EXTERNAL_TABLE",
            "StorageDescriptor": {
                "Location": "{0}/{1}".format(s3_location, tablename),
                "Columns": column_definition_list,
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "Parameters": {"serialization.format": "1"}
                }
            },
            "PartitionKeys": partitionkeys
        }
    }
    return schema_dict


def sync_table_column_schemas(base_table, new_table):
    """
    This will update new_table to have the same data types as the corresponding column in base_table.
    This doesn't affect new columns.
    Use this to make sure that new partition definitions match the base table definition for known columns.

    :param base_table: table schema that contains the correct datatypes
    :param new_table: new table schema to be updated.
    :return: schema where all columns that exist in both schemas have the same data type as base_table
    """
    base_columns = base_table["Table"]["StorageDescriptor"]["Columns"]
    new_columns = new_table["Table"]["StorageDescriptor"]["Columns"]
    for column in new_columns:
        if column not in base_columns:
            column_name = column["Name"]
            for col in base_columns:
                if col["Name"] == column_name:
                    base_type = col["Type"]
                    column["Type"] = base_type
    return new_table


def cast_dataframe_to_schema(df, target_schema):
    target_schema_columns = target_schema["Table"]["StorageDescriptor"]["Columns"]
    df_schema_columns = df.dtypes
    for target_column in target_schema_columns:
        column_name = target_column["Name"]
        target_schema_column_type = target_column["Type"]
        for df_column in df_schema_columns:
            if df_column[0] == column_name:
                df_schema_column_type = df_column[1]
                if df_schema_column_type != target_schema_column_type:
                    log("casting {0} to {1}".format(column_name, target_schema_column_type))
                    column_temp_name = "temporary_name_{0}_todelete".format(column_name)
                    df = df.withColumn(column_temp_name, col(column_name).cast(target_schema_column_type)).drop(
                        column_name)
                    df = df.withColumnRenamed(column_temp_name, column_name)
    return df


def write_frame_to_dynamic_partition(df, tableschema, tempfile_location, compression_format="snappy",
                                     maxrecords=1500000):
    """
    :param df: dataframe to write
    :param tableschema: glue catalog compatible schema definition of
    :param tempfile_location: temp s3 location to write files before copying to table schema
    :param compression_format: snappy or gzip TBI
    :param maxrecords: max number of records per file
    (can generate this with create_catalog_schema_from_dataframe)
    :return:
    """
    partition_schema = tableschema["Table"]["PartitionKeys"]
    partition_definition = []
    for column in partition_schema:
        partition_definition.append(column["Name"])
    # write dataframe to S3
    df.repartition(1).write. \
        options(compression=compression_format, maxRecordsPerFile=maxrecords). \
        partitionBy(partition_definition). \
        parquet(tempfile_location)
    return


def copy_temp_files_to_table(source_location, dest_location, file_signature):
    session = boto3.Session()
    s3 = session.resource('s3')
    # source variables
    source_loc_urlparse = urlparse(source_location)
    source_bucket_name = source_loc_urlparse.netloc
    source_bucket_filter = source_loc_urlparse.path.lstrip('/')
    source_bucket = s3.Bucket(source_bucket_name)
    # dest variables
    dest_loc_urlparse = urlparse(dest_location)
    dest_bucket_name = dest_loc_urlparse.netloc
    output_file_list = []
    for object_summary in source_bucket.objects.filter(Prefix=source_bucket_filter):
        copy_source = {"Bucket": source_bucket_name, "Key": object_summary.key}
        # get new file name and path
        original_file_list = object_summary.key.split('/')
        original_file_name = original_file_list[-1]
        new_file_name = original_file_name.replace(original_file_name.split('.')[0], file_signature)
        new_file_list = original_file_list[2:-1]
        new_file_path = ""
        for key in new_file_list:
            new_file_path = "{0}/{1}".format(new_file_path, key)
        new_file = "{0}/{1}".format(new_file_path, new_file_name).lstrip('/')
        s3.meta.client.copy(copy_source, dest_bucket_name, new_file)
        output_file_list.append(new_file)
    return output_file_list


def get_config_from_catalog(database_name, table_name, glue_client):
    try:
        response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        del response["Table"]["DatabaseName"]
        response["DatabaseName"] = database_name
    except glue_client.exceptions.EntityNotFoundException:
        response = None
    return response

class GlueJobInstance(GlueShellJob, object):
    def __init__(self, jobname, env_configuration_path, job_start, job_end):
        super(GlueJobInstance, self).__init__(jobname, env_configuration_path, job_start, job_end)
        # initialize spark and glue
        self.sc = SparkContext()
        self.glue_context = GlueContext(self.sc)
        self.glue_context._jsc.hadoopConfiguration().set("fs.s3.canned.acl", "BucketOwnerFullControl")
        self.spark = self.glue_context.spark_session
        # initialize config variables
        self.raw_database = self.job_config["RawDatabase"]
        self.table_list = self.job_config["RawTableList"]
        self.column_filter = self.job_config["ColumnFilter"]
        self.dest_database = self.job_config["DatabaseName"]
        self.dest_s3location = self.job_config["S3Destination"]
        self.workspace = "{0}/{1}".format(self.workspace_bucket, self.job_history_id)

    def job_process(self):
        partition_date = datetime.strftime(self.current_process_dt, "%Y-%m-%d")
        partition_hour = datetime.strftime(self.current_process_dt, "%H")
        partition_hour_int = int(partition_hour)
        pushdown_predicate = "upload_dt=='{0}' AND hour=='{1}'".format(partition_date, partition_hour)
        for table in self.table_list:
            log("processing table {0}".format(table))
            dyf = self.glue_context.create_dynamic_frame.from_catalog(self.raw_database, table,
                                                                      push_down_predicate=pushdown_predicate)

            # add resolves for top-level columns and static structs (i.e., api_properties, library) here
            dyf = ResolveChoice.apply(dyf, specs=[("sequence_number", "cast:bigint"),
                                                  ("session_id", "cast:bigint"),
                                                  ("event_type", "cast:string"),
                                                  ("user_id", "cast:string"),
                                                  ("device_id", "cast:string"),
                                                  ("uuid", "cast:string"),
                                                  ("platform", "cast:string"),
                                                  ("event_id", "cast:int"),
                                                  ("time", "cast:timestamp"),
                                                  ("timestamp", "cast:timestamp"),
                                                  ("sequence_number", "cast:bigint"),
                                                  ("hour", "cast:int")
                                                  ])

            dyf = resolve_choices_in_structs(dyf, "event_properties")
            dyf = resolve_choices_in_structs(dyf, "user_properties")

            dfc = Relationalize.apply(frame=dyf, staging_path="{0}/relationalize".format(self.workspace),
                                      name="root",
                                      transformation_ctx="dfc")
            dyf_root = dfc.select('root')
            df = dyf_root.toDF()

            if df.count() > 0:
                # DROP COLUMNS THAT ARE HANDLED IN THE USER DEVICE MAP
                delete_list = ["library", "group_properties", "device_manufacturer", "device_model", "device_brand",
                               "country", "language", "carrier", "os_name", "os_version", "version_name",
                               "api_properties"]
                for column in df.columns:
                    column_full_name_list = column.split('.')
                    if column_full_name_list[0] in delete_list:
                        df = df.drop(column)
                    else:
                        # CONVERT STRUCTS TO TOP LEVEL COLUMNS AND CLEAN UP NAMES
                        # flatten the column name and try to remove the top struct name from it
                        if len(column_full_name_list) > 1:
                            flattened_column_name = re.sub("[^0-9a-zA-Z]+", "_", column).lower()
                            proposed_column_name = flattened_column_name[len(column_full_name_list[0]) + 1:]

                            # filter out blacklisted columns
                            if proposed_column_name not in self.column_filter:
                                if proposed_column_name not in df.columns:
                                    df = df.withColumnRenamed(column, proposed_column_name)
                                else:
                                    df = df.withColumnRenamed(column, flattened_column_name)
                            else:
                                df = df.drop(column)

                # REMOVE DUPLICATES
                if "approx_arrival_time" not in df.columns:
                    if "upload_time" in df.columns:
                        df = df.withColumn("approx_arrival_time", df.upload_time.cast("timestamp"))
                    else:
                        df = df.withColumn("approx_arrival_time", df.upload_dt.cast("timestamp"))

                if "insert_id" in df.columns:
                    df = df.withColumn("row_number", row_number().over(
                        Window.partitionBy(df.insert_id).orderBy(df.approx_arrival_time.asc()))).filter(
                        col("row_number") == 1).drop("row_number")
                elif "uuid" in df.columns:
                    df = df.withColumn("row_number", row_number().over(
                        Window.partitionBy(df.uuid).orderBy(df.approx_arrival_time.asc()))).filter(
                        col("row_number") == 1).drop("row_number")
                else:
                    df = df.distinct()

                # ADD CLIENT EVENT TIMESTAMP
                if "time" in df.columns:
                    df = df.withColumn("event_utc_timestamp", df.time)
                    df = df.drop("time")
                elif "timestamp" in df.columns:
                    df = df.withColumn("event_utc_timestamp", df.timestamp)
                    df = df.drop("timestamp")
                else:
                    df = df.withColumn("event_utc_timestamp", df.approx_arrival_time)

                # ADD UPLOAD TIMESTAMP
                df = df.withColumn("upload_utc_timestamp", df.approx_arrival_time.cast("timestamp"))
                df = df.drop("approx_arrival_time")

                # CONVERT NULL COLUMNS TO STRING
                for column in df.dtypes:
                    colname = column[0]
                    coltype = column[1]
                    if coltype == "null":
                        if colname == "user_properties":
                            df = df.drop("user_properties")
                        else:
                            temp_name = "{0}_tempnullconvert".format(colname)
                            df = df.withColumn(temp_name, col(colname).cast("string")).drop(colname)
                            df = df.withColumnRenamed(temp_name, colname)

                # CREATE A TABLE DEFINITION
                partitions = [{"Name": "upload_dt", "Type": "date"}, {"Name": "hour", "Type": "int"}]
                dataframe_schema = create_catalog_schema_from_dataframe(df, str(table), self.dest_s3location,
                                                                        partitions)

                catalog_table_schema = get_config_from_catalog(self.dest_database, table, self.glue_client)
                if catalog_table_schema is not None:
                    synced_schema = sync_table_column_schemas(catalog_table_schema, dataframe_schema)
                    df = cast_dataframe_to_schema(df, catalog_table_schema)
                else:
                    synced_schema = dataframe_schema

                synced_schema["DatabaseName"] = self.dest_database
                data_model = CatalogObject(synced_schema, self.glue_client, self.glue_context)
                # WRITE DATAFRAME TO S3
                df = df.sort('user_id', ascending=False)
                self.rowcount = df.count()

                # first write to temp storage
                log("writing files to temp storage")
                unique_file_location = str(uuid.uuid4())
                temp_file_location = "{0}/{1}/{2}".format(self.workspace, unique_file_location, table)
                final_file_location = "{0}/{1}".format(self.dest_s3location, table)
                write_frame_to_dynamic_partition(df, dataframe_schema, temp_file_location)

                # now rename files and copy to the table directory
                log("copying files to table")
                file_signature = "{0}-{1}".format(partition_date, partition_hour)
                copy_temp_files_to_table(temp_file_location, final_file_location, file_signature)
                partition_path = "upload_dt={0}/hour={1}".format(partition_date, partition_hour_int)
                partition_values = [partition_date, str(partition_hour_int)]
                data_model.add_partition(partition_path, partition_values)
                # mark the partition for this table complete in the state tracker
                tracked_object = "{0}.{1}".format(self.dest_database, table)
                self.job_stop_flag = self.statetracker.track_job_progress(self.job_history_id,
                                                                          self.current_process_dt,
                                                                          trackedobject=tracked_object,
                                                                          rowcount=self.rowcount)

        log("iteration complete")


# INITIALIZE JOB
args = getResolvedOptions(sys.argv, ['env_config_path', 'job_name', 'job_start_datetime', 'job_end_datetime'])
glue_job = GlueJobInstance(args['job_name'], args['env_config_path'], job_start=args['job_start_datetime'],
                           job_end=args['job_end_datetime'])

# RUN JOB
glue_job.run_job()
