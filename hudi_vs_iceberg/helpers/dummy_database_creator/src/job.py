import os

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from test_data_generator.TestDataGeneratorLib import TestDataGeneratorLib
from test_data_generator.TestDataGeneratorTarg import TestDataGeneratorTarg
from utils.helpers import (
    construct_warehouse_path_from_config,
    get_config,
    validate_set_format_against_config,
)
from utils.session_builder import SessionBuilder

# Initialization
set_format = os.environ.get("DATALAKE_FORMATS", "")
config_filename = os.environ.get("CONFIG_FILENAME", "")

config_file = get_config(set_format, config_filename)
validate_set_format_against_config(set_format, config_file)

warehouse_path = construct_warehouse_path_from_config(config_file)
session_builder_kwargs = (
    {"warehouse_path": warehouse_path} if set_format == "iceberg" else {}
)

spark_session = SessionBuilder.build(set_format, **session_builder_kwargs)
sc = spark_session.sparkContext
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(f"test_{set_format}")

# Test Data Generator main  function


def _main_test_data_generator():
    """
    This function is the main entry point for the Test Data Generator
    application. It uses the TestDataGeneratorLib and TestDataGeneratorTarg
    classes to generate test data and write it to output targets.

    The function expects a YAML configuration file that contains the
    following parameters:
    - number_of_generated_records: The number of records to generate.
    - attributes_list: A list of attributes to be generated, along with
      their configuration details.
    - target_list: A list of output targets, along with their configuration
      details.

    :return: None
    """
    number_of_generated_records = config_file["number_of_generated_records"]
    tgd = TestDataGeneratorLib(spark, number_of_generated_records)
    tgd_targets = TestDataGeneratorTarg(glueContext)

    target_config = config_file["target_list"][0]
    target_attributes = target_config["attributes"]
    tgd_targets.cleanup_existing_resources(target_attributes)
    generated_df = spark.range(0, number_of_generated_records, 1)
    generated_df = generated_df.withColumn("id", col("id").cast(StringType()))

    # Test Data Generation
    for atribute_config in config_file["attributes_list"]:
        fn = getattr(tgd, atribute_config["Generator"])
        generated_df = fn(
            generated_df,
            atribute_config["DataDescriptor"],
            atribute_config["ColumnName"],
        )

    generated_df = generated_df.drop("id")

    optional_arguments = (
        {"attributes_list": config_file["attributes_list"]}
        if set_format == "iceberg"
        else {}
    )

    # Test Output writing
    fn = getattr(tgd_targets, target_config["target"])
    fn(generated_df, target_attributes, **optional_arguments)

    job.commit()


_main_test_data_generator()
