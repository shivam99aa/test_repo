from datetime import datetime

from pyspark.sql.functions import lit
from pyspark.sql.types import *


def log(log_string):
    log_dt = datetime.now()
    logval = "{0} Life360LogEntry - {1}".format(log_dt, log_string)
    print(logval)
    return


def checkpoint_dataframe(spark, df, partitions, file_format, checkpoint_location):
    df.repartition(partitions).write.format(file_format).options(compression="gzip").mode("overwrite").save(
        checkpoint_location)
    if file_format == "json":
        new_df = spark.read.option("header", "true").json(checkpoint_location)
    elif file_format == "parquet":
        new_df = spark.read.option("header", "true").parquet(checkpoint_location)
    else:
        new_df = spark.read.option("header", "true").csv(checkpoint_location)
    return new_df


def checkpoint_dataframe_with_schema(spark, df, partitions, file_format, checkpoint_location, jobname, sch):
    checkpoint_path = "{0}/{1}/{2}".format(checkpoint_location, jobname,
                                           datetime.strftime(datetime.now(), "%Y%m%d%H%M%S%f"))
    df.repartition(partitions).write.format(file_format).options(compression="gzip").mode("overwrite").save(
        checkpoint_path)
    if file_format == "json":
        new_df = spark.read.schema(sch).option("header", "true").json(checkpoint_path)
    elif file_format == "parquet":
        new_df = spark.read.schema(sch).option("header", "true").parquet(checkpoint_path)
    else:
        new_df = spark.read.schema(sch).option("header", "true").csv(checkpoint_path)
    return new_df


def checkpoint_dynamicframe(glue_context, df, fileformat, workspace, jobname):
    checkpoint_path = "{0}/{1}/{2}".format(workspace, jobname,
                                           datetime.strftime(datetime.now(), "%Y%m%d%H%M%S%f"))
    glue_context.write_dynamic_frame.from_options(frame=df,
                                                  connection_type="s3",
                                                  connection_options={"path": checkpoint_path},
                                                  format=fileformat)
    new_df = glue_context.create_dynamic_frame.from_options('s3', {"paths": [checkpoint_path]}, fileformat)
    return new_df


def get_config_from_catalog(database_name, table_name, glue_client):
    response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
    del response["Table"]["DatabaseName"]
    response["DatabaseName"] = database_name
    return response


class CatalogObject:
    def __init__(self, config, client, context):
        log("initializing CatalogObject for {0}".format(config["Table"]["Name"]))
        self.tableDefinition = config["Table"]
        self.s3DestinationLocation = config["Table"]["StorageDescriptor"]["Location"]
        self.databaseName = config["DatabaseName"]
        self.tableName = config["Table"]["Name"]
        self.storageDescriptor = config["Table"]["StorageDescriptor"]
        self.columns = self.storageDescriptor["Columns"]
        self.glueClient = client

    def create_table(self):
        log("checking table: {0}".format(self.tableName))
        # check if table already exists
        try:
            result = "table {0} already exists".format(self.tableName)
            self.glueClient.get_table(DatabaseName=self.databaseName, Name=self.tableName)
        # table not found, proceed to create table
        except self.glueClient.exceptions.EntityNotFoundException:
            response = self.glueClient.create_table(
                DatabaseName=self.databaseName,
                TableInput=self.tableDefinition
            )
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                result = "table {0} created".format(self.tableName)
            else:
                log("create table {0} response is: {1}".format(self.tableName, response))
                result = "table create for {0} failed; check log for details".format(self.tableName)

        log(result)
        return result

    def add_partition(self, partition_path, partition_value):
        partition_values = []
        if type(partition_value) == list and len(partition_value) >= 1:
            partition_values = partition_value
        else:
            partition_values = [partition_value]

        log("checking partition {0} for table {1}".format(partition_value, self.tableName))
        # check if table exists and create if needed
        self.create_table()
        try:
            logresult = "partition {0} already exists for table {1}".format(partition_value, self.tableName)
            result = 0
            self.glueClient.get_partition(
                DatabaseName=self.databaseName,
                TableName=self.tableName,
                PartitionValues=partition_values)
        # partition not found, proceed to create partition
        except self.glueClient.exceptions.EntityNotFoundException:
            # add to catalog
            storage = self.storageDescriptor.copy()
            storage["Location"] = self.storageDescriptor["Location"] + "/{0}".format(partition_path)
            partition_input = {
                "Values": partition_values,
                "StorageDescriptor": storage
            }
            response = self.glueClient.create_partition(
                DatabaseName=self.databaseName,
                TableName=self.tableName,
                PartitionInput=partition_input
            )
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                logresult = "partition {0} created for table {1}".format(partition_value, self.tableName)
                result = 0
            else:
                log("create partition {0} response is: {1}".format(partition_value, response))
                logresult = "partition create {0} for table {1} failed; check log for details".format(
                    partition_value, self.tableName)
                result = 1

        log(logresult)
        return result

    @staticmethod
    def map_type(sql_type):
        spark_type = {
            "timestamp": TimestampType(),
            "bigint": LongType(),
            "int": IntegerType(),
            "string": StringType(),
            "double": DoubleType(),
            "boolean": BooleanType(),
            "float": FloatType(),
            "array<int>": ArrayType(IntegerType()),
            "map<string,string>": MapType(StringType(), StringType())
        }
        return spark_type.get(sql_type, StringType())

    def generate_schema(self, config):
        column_type = [d.get('Type', None) for d in config]
        column_name = [d.get('Name', None) for d in config]
        catalog_struct = StructType()
        length = len(config)
        for i in range(length):
            if column_name[i] != 'event_properties':
                new_field = StructField(column_name[i], self.map_type(column_type[i]), True)
                catalog_struct.add(new_field)
        return catalog_struct

    def sort_dataframe(self, df):
        column_list = [d.get('Name', None) for d in self.tableDefinition["StorageDescriptor"]["Columns"]]
        sorted_df = df.select(column_list)
        return sorted_df

    def add_missing_columns_to_dataframe(self, df):
        column_type = [d.get('Type', None) for d in self.columns]
        column_name = [d.get('Name', None) for d in self.columns]
        length = len(self.columns)
        for i in range(length):
            if column_name[i] not in df.columns:
                df = df.withColumn(column_name[i], lit("").cast(column_type[i]))
        return df

    def write_frame_to_table_partition(self, df, partition_value, partition_path, partition_num=1,
                                       writemode="overwrite", writeformat="parquet", compression_format="gzip"):
        # writemode = "append" to add a file
        log("writing dataframe to partition {0} for table {1}".format(partition_value, self.tableName))
        # write dataframe to S3
        if writeformat == "parquet":
            df.repartition(partition_num).\
                write.\
                format(writeformat).\
                options(compression=compression_format).\
                mode(writemode).\
                save("{0}/{1}".format(self.s3DestinationLocation, partition_path))
        else:
            # specify timestamp format when writing csv and json
            df.repartition(partition_num).\
                write.\
                format(writeformat).\
                options(compression=compression_format, timestampFormat="yyyy-MM-dd HH:mm:ss.SSS").\
                mode(writemode).\
                save("{0}/{1}".format(self.s3DestinationLocation, partition_path))

        # add partition to table
        self.add_partition(partition_path, partition_value)
        return

    def write_frame_to_snowflake(self, sf_options, df):
        log("writing dataframe to snowflake for table {0} in database {1}".format(self.tableName, self.databaseName))
        df.write.format("net.snowflake.spark.snowflake") \
            .options(**sf_options) \
            .options(timestampFormat="yyyy-MM-dd HH:mm:ss.SSS") \
            .option("sfDatabase", self.databaseName) \
            .option("sfSchema", "PUBLIC") \
            .option("dbtable", self.tableName) \
            .mode("append") \
            .save()
        return

