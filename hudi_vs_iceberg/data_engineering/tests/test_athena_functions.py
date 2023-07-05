import awswrangler as wr
from glue_jobs.athena_functions import * 
import pandas as pd
from tests.compare_df import assert_df_equality

bucket_name = "sb-test-bucket-ireland"
key = "tpcds_test"
directory = f"s3://{bucket_name}/{key}"
database_name = "tpcds_test"
compute = "athena_iceberg"

dest_test_table_name = f"{compute}_dest_table"
dest_test_folder = f"{directory}/{compute}/dest_table"
dest_parquet_folder = f"{dest_test_folder}/data"
future_end_datetime = '2250-01-01'
primary_key = "pk"

def test_bulk_insert():
    
    # Arrange
    fl_table_name = "bulk_insert"
    result_table_name = "bulk_insert"
    
    # Act 
    results = bulk_insert(bucket_name, database_name, dest_test_table_name, dest_test_folder, future_end_datetime, database_name, fl_table_name)
    
    # Assert
    for result in results:
        assert result['Status']['State'] == 'SUCCEEDED'

    actual_df = wr.athena.read_sql_query(f"SELECT * FROM {dest_test_table_name}", database=database_name, ctas_approach=False, workgroup='Athena3')
    expected_df = wr.athena.read_sql_query(f"SELECT * FROM {result_table_name}", database=database_name, ctas_approach=False, workgroup='Athena3')
    assert_df_equality(expected_df,actual_df)

    
def test_scd2_simple_update():
    
    # Arrange
    cdc_table_name = "cdc_1"
    result_table_name = "update_1"
    
    # Act 
    results = scd2_simple(database_name, dest_test_table_name, database_name, cdc_table_name, primary_key, future_end_datetime)

    # Assert
    for result in results:
        assert result['Status']['State'] == 'SUCCEEDED'

    actual_df = wr.athena.read_sql_query(f"SELECT * FROM {dest_test_table_name}", database=database_name, ctas_approach=False, workgroup='Athena3')
    expected_df = wr.athena.read_sql_query(f"SELECT * FROM {result_table_name}", database=database_name, ctas_approach=False, workgroup='Athena3')
    assert_df_equality(expected_df, actual_df)
    

def test_scd2_simple_insert():
    
    # Arrange
    cdc_table_name = "cdc_2"
    result_table_name = "update_2"

    # Act 
    results = scd2_simple(database_name, dest_test_table_name, database_name, cdc_table_name, primary_key, future_end_datetime)

    # Assert
    for result in results:
        assert result['Status']['State'] == 'SUCCEEDED'

    actual_df = wr.athena.read_sql_query(f"SELECT * FROM {dest_test_table_name}", database=database_name, ctas_approach=False, workgroup='Athena3')
    expected_df = wr.athena.read_sql_query(f"SELECT * FROM {result_table_name}", database=database_name, ctas_approach=False, workgroup='Athena3')
    assert_df_equality(expected_df,actual_df)

    
def test_scd2_complex():
    
    # Arrange
    fl_table_name = "bulk_insert"
    cdc_table_name = "cdc_3"
    result_table_name = "update_3"
    use_case = "scd2_complex"

    # Act 
    results = scd2_complex(database_name, dest_test_table_name, database_name, cdc_table_name, primary_key, future_end_datetime)

    # Assert
    for result in results:
        assert result['Status']['State'] == 'SUCCEEDED'

    actual_df = wr.athena.read_sql_query(f"SELECT * FROM {dest_test_table_name}", database=database_name, ctas_approach=False, workgroup='Athena3')
    expected_df = wr.athena.read_sql_query(f"SELECT * FROM {result_table_name}", database=database_name, ctas_approach=False, workgroup='Athena3')
    assert_df_equality(expected_df,actual_df)

    