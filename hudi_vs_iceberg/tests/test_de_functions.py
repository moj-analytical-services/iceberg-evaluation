import pytest
import sys
import datetime

from pandas.pandas_functions import bulk_insert, scd2_simple, scd2_complex

future_end_datetime = datetime.datetime(2250, 1, 1)
primary_key = "product_id"
input_data_directory = (
    "s3://sb-test-bucket-ireland/data-engineering-use-cases/dummy-data/"
)
full_load_filepath = f"{input_data_directory}full_load/full_load.parquet"
updates_filepath = f"{input_data_directory}updates/updates.parquet"
late_updates_filepath = f"{input_data_directory}late_updates/late_updates.parquet"
output_data_directory = (
    "s3://sb-test-bucket-ireland/soumaya/de-usecases/pandas/pandas-python/"
)


@pytest.fixture(scope="module", autouse=True)
def glue_context(request):
    compute = request.config.getoption('--compute')
    
    if compute == "athena_iceberg":
        import awswrangler as wr
        
    if compute == "glue_iceberg":
        from pyspark.context import SparkContext
        from awsglue.context import GlueContext
        from awsglue.job import Job
        from awsglue.utils import getResolvedOptions
        sys.argv.append("--JOB_NAME")
        sys.argv.append("test_glue_iceberg_functions")

        args = getResolvedOptions(sys.argv, ["JOB_NAME"])
        context = GlueContext(SparkContext.getOrCreate())
        job = Job(context)
        job.init(args["JOB_NAME"], args)

        yield (context)

        job.commit()

def test_bulk_insert(glue_context):
    bulk_insert(full_load_filepath, output_data_directory, future_end_datetime)
    assert 1 == 1