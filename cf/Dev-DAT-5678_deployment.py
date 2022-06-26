import sys
from pyspark.sql.functions import from_json, col
from awsglue.utils import getResolvedOptions
from awsglue.transforms import Relationalize, ResolveChoice, DropFields
from functools import reduce
# pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, FloatType,ArrayType
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from datetime import datetime, date, timedelta
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
import boto3
import time
import logging
import s2sphere
import pandas as pd
from pytz import timezone
from timezonefinder import TimezoneFinder


utc = timezone('UTC')
date = datetime.utcnow()

instance = [None]


def map_fields(rec):
    rec["s2_cell"] =""
    rec["s2_cell_neighbors"] = ""
    rec["utc_offset"] = ""

    if "latitude" in rec or "longitude" in rec:
        lat_lng           = s2sphere.LatLng.from_degrees(rec['latitude'],rec['longitude'])
        utc_offset = ''
        cell              = s2sphere.CellId.from_lat_lng(lat_lng)
        s2_cell           = str(cell.parent(19))
        s2_cell           = s2_cell.replace('CellId: ','')
        s2_cell_neighbors = [str(x).replace('CellId: ','') for x in list(cell.parent(19).get_all_neighbors(19))[:5]]
        if instance[0] is None:
            instance[0] = TimezoneFinder()
        tzf = instance[0]
        try:
            timezone_str = tzf.timezone_at(lat=rec['latitude'], lng=rec['longitude'])
            if timezone_str:
                tz  = timezone(timezone_str)
                v2 = utc.localize(date).astimezone(tz)
                time_zone = v2.strftime('%z')
                utc_offset = time_zone[:3]+':'+time_zone[3:]
        except Exception as e:
            print(e)
        rec["s2_cell"] = s2_cell
        rec["s2_cell_neighbors"] =    s2_cell_neighbors
        rec["utc_offset"] = utc_offset

    return rec

if __name__ == '__main__':
    # # # initiate job

    spark = SparkSession.builder \
        .appName('dwell_backfill_etl') \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.crossJoin.enabled", "true") \
        .config("spark.driver.memory", "3g") \
        .config("spark.driver.maxResultSize", "3g") \
        .config("spark.sql.broadcastTimeout", 7200) \
        .config("mapred.output.committer.class","org.apache.hadoop.mapred.DirectFileOutputCommitter") \
        .getOrCreate()

    glueContext = GlueContext(spark.sparkContext)
    dyf = glueContext.create_dynamic_frame_from_options('s3', {
        'paths': ['s3://life360-glue-workspace-dev/dwells/dt=2022-05-16/hour=00/'],
        'groupFiles': 'inPartition'
    }, format='json')


    # 	dyf = glueContext.create_dynamic_frame_from_options('s3', {
    # 				'paths': ['s3://life360-glue-workspace-dev/dwells/sample_data/'],
    # 				'groupFiles': 'inPartition'
    # 			}, format='json')

    dyf = ResolveChoice.apply(
        dyf,
        specs=[
            ('latitude', 'cast:double'),
            ('longitude', 'cast:double'),
            ('start_time', 'cast:timestamp'),
            ('end_time', 'cast:timestamp')
        ]
    )
    dyf = Map.apply(frame=dyf, f=map_fields)



    # 	schema = StructType([StructField("cell", StringType(), False),StructField("cell_neighbors", ArrayType(StringType(), False)),StructField("utc_offset", StringType(), False)])
    df = dyf.toDF()
    df = df.dropDuplicates()
    # 	col_names = df.columns
    # # 	udf_attributes = udf(lambda x,y: get_udf_attr(x,y), schema)
    # # 	df = df.withColumn('udf_attr',get_udf_attr(df.latitude,df.longitude))
    # # 	df = df.groupBy().apply(get_udf_attr(df.latitude,df.longitude))
    # 	df = df.groupby('latitude','longitude').apply(get_udf_attr)
    # 	df.show(10)
    # 	df = df.select(col("udf_attr.cell").alias("s2_cell_id"),col("udf_attr.cell_neighbors").alias("s2_cell_neighbors_ids"),col("udf_attr.utc_offset").alias("utc_offset"),*col_names)
    # # 	df = df.select('udf_attr')
    df.write.format("parquet").option("compression", "gzip").mode('overwrite').save('s3://life360-glue-workspace-dev/dwells/output/dt=2022-05-16')


