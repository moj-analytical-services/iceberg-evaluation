import pytest
import sys
import pandas as pd
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkConf
from tests.compare_df import assert_df_equality

bucket_name = "sb-test-bucket-ireland"
key = "tpcds_test"
directory = f"s3://{bucket_name}/{key}"
compute = "glue_hudi"
database_name = "tpcds_test"
catalog_name = "glue_catalog"

@pytest.fixture(scope="module", autouse=True)
def glue_context():
    sys.argv.append('--JOB_NAME')
    sys.argv.append('test_count')

    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    conf = SparkConf()
    conf.set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set(f"spark.sql.catalog.{catalog_name}.warehouse", f"{database_name}") 
    conf.set(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") 
    conf.set(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") 
    conf.set(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") 
    context = GlueContext(SparkContext.getOrCreate(conf=conf))
    job = Job(context)
    job.init(args['JOB_NAME'], args)

    yield(context)

    job.commit()


def test_bulk_insert(glue_context):
    spark_df = glue_context.create_data_frame.from_catalog(
        database=database_name,
        table_name="bulk_insert",
        additional_options={"useCatalogSchema": True},
    )
    actual_df = spark_df.toPandas()
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
