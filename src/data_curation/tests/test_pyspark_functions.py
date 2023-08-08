
import pytest
import sys
import pandas as pd
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkConf
from glue_jobs.pyspark_functions import * 
from tests.compare_df import assert_df_equality
import datetime

from pyspark.sql import SparkSession, functions as F
from pyspark.sql import SparkSession

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, Row
from pyspark.context import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import time



bucket_name = "sb-test-bucket-ireland"
key = "tpcds_test"
compute = "glue_iceberg"
database_name = "tpcds_test"
bucket_prefix = "sb"
table_name = "datagensb"
warehouse_path = f"s3://{bucket_name}/{bucket_prefix}"
directory = f"s3://{bucket_name}/{key}"


catalog_name = "glue_catalog"
bucket_name = "sb-test-bucket-ireland"
bucket_prefix = "sb"
table_name = "datagensb"
warehouse_path = f"s3://{bucket_name}/{bucket_prefix}"
input_prefix = "tpcds_test"
input_path = f"s3://{bucket_name}/{input_prefix}"
source_database_name = "tpcds_test"
dest_database_name = "tpcds_test_glue_iceberg"
output_directory = f"{catalog_name}.{dest_database_name}.{table_name}"
future_end_datetime = datetime.datetime(2250, 1, 1)


spark = SparkSession.builder \
    .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"{warehouse_path}") \
    .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()






@pytest.fixture(scope="module")
def glue_context():
    sys.argv.append('--JOB_NAME')
    sys.argv.append('test_count')

    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    conf = SparkConf()
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.sql.hive.convertMetastoreParquet","false")
    context = GlueContext(SparkContext.getOrCreate(conf=conf))
    job = Job(context)
    job.init(args['JOB_NAME'], args)

    yield(context)

    job.commit()


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.config(
            f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"{warehouse_path}")
        .config(
            f"spark.sql.catalog.{catalog_name}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config(
            f"spark.sql.catalog.{catalog_name}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .getOrCreate()
    )
    yield spark
    #spark.stop()

   

def test_bulk_insert(glue_context,spark):

    
    bulk_insert(f"{input_path}/full_load", output_directory, future_end_datetime,spark)
    #output_directory = f'{output_directory}_bulkinsert'
    actual_df = pd.read_parquet(f"s3://{bucket_name}/{input_prefix}/bulk_insert")
    #expected_df = pd.read_parquet(output_directory)
    expected_df = spark.table(output_directory).toPandas()
    assert_df_equality(expected_df,actual_df)
    
def test_scd2_simple_update(glue_context,spark):
    scd2_simple(f"{input_path}/cdc_1", output_directory, future_end_datetime, "pk",spark)
    actual_df = pd.read_parquet(f"s3://{bucket_name}/{input_prefix}/update_1")
    expected_df = spark.table(output_directory).toPandas()
    assert_df_equality(expected_df,actual_df)
    
def test_scd2_simple_insert(glue_context,spark):
    scd2_simple(f"{input_path}/cdc_2", output_directory, future_end_datetime, "pk",spark)
    actual_df = pd.read_parquet(f"s3://{bucket_name}/{input_prefix}/update_2")
    expected_df = spark.table(output_directory).toPandas()
    assert_df_equality(expected_df,actual_df)
    
def test_scd2_complex(glue_context,spark):
    scd2_complex(f"{input_path}/cdc_3", output_directory, future_end_datetime, "pk",spark)
    actual_df = pd.read_parquet(f"s3://{bucket_name}/{input_prefix}/update_3")
    expected_df = spark.table(output_directory).toPandas()
    assert_df_equality(expected_df,actual_df)
