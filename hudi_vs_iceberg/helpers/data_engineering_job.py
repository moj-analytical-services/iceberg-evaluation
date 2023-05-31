import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

compute = "glue_pandas"
use_case = "bulk_insert"
scripts_path = "s3://sb-test-bucket-ireland/data-engineering-use-cases/"

future_end_datetime = datetime.datetime(2250, 1, 1)
primary_key = "product_id"
input_data_directory = (
    "s3://sb-test-bucket-ireland/data-engineering-use-cases/dummy-data/"
)
full_load_filepath = f"{input_data_directory}full_load/full_load.parquet"
updates_filepath = f"{input_data_directory}updates/updates.parquet"
late_updates_filepath = f"{input_data_directory}late_updates/late_updates.parquet"
output_data_directory = f"s3://sb-test-bucket-ireland/data-engineering-use-cases/{compute}/"

if compute == "glue_hudi":
    sc.addPyFile(f"{scripts_path}glue_hudi_functions.py")
    from glue_hudi_functions import bulk_insert, scd2_complex, scd2_simple

if compute == "glue_iceberg":
    sc.addPyFile(f"{scripts_path}glue_iceberg_functions.py")
    from glue_iceberg_functions import bulk_insert, scd2_complex, scd2_simple

if use_case == "bulk_insert":
    bulk_insert_filepath = bulk_insert(
        full_load_filepath, output_data_directory, future_end_datetime
    )

if use_case == "scd2_simple":
    scd2_simple_filepath = scd2_simple(
        bulk_insert_filepath,
        updates_filepath,
        output_data_directory,
        future_end_datetime,
        primary_key,
    )

if use_case == "scd2_complex":
    scd2_complex_filepath = scd2_complex(
        scd2_simple_filepath,
        late_updates_filepath,
        output_data_directory,
        future_end_datetime,
        primary_key,
    )
