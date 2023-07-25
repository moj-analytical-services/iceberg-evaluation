
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








bucket_name = "sb-test-bucket-ireland"
key = "tpcds_test"
compute = "glue_iceberg"
database_name = "tpcds_test"
bucket_prefix = "sb"
table_name = "datagensb"
warehouse_path = f"s3://{bucket_name}/{bucket_prefix}"
directory = f"s3://{bucket_name}/{key}"






@pytest.fixture(scope="module", autouse=True)
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


def test_bulk_insert(glue_context):
    actual_df = pd.read_parquet(f"{directory}/bulk_insert")
    expected_df = pd.read_parquet(f"{directory}/bulk_insert")
    assert_df_equality(expected_df,actual_df)
    
def test_scd2_simple_update(glue_context):
    actual_df = pd.read_parquet(f"{directory}/update_1")
    expected_df = pd.read_parquet(f"{directory}/update_1")
    assert_df_equality(expected_df,actual_df)
    
def test_scd2_simple_insert(glue_context):
    actual_df = pd.read_parquet(f"{directory}/update_2")
    expected_df = pd.read_parquet(f"{directory}/update_2")
    assert_df_equality(expected_df,actual_df)
    
def test_scd2_complex(glue_context):
    actual_df = pd.read_parquet(f"{directory}/update_3")
    expected_df = pd.read_parquet(f"{directory}/update_3")
    assert_df_equality(expected_df,actual_df)
