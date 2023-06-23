import pytest
import sys
import datetime
import awswrangler as wr
import boto3
import os
import logging
import pydbtools as pydb
import pandas as pd
from tests.compare_df import assert_df_equality

bucket_name = "sb-test-bucket-ireland"
key = "tpcds_test"
directory = f"s3://{bucket_name}/{key}"
database_name = "tpcds_test"
compute = "athena_iceberg"

def test_bulk_insert():
    actual_df = wr.athena.read_sql_query("SELECT * FROM bulk_insert", database=database_name)
    expected_df = pd.read_parquet(f"{directory}/bulk_insert")
    assert_df_equality(expected_df,actual_df)
    
def test_scd2_simple_update():
    actual_df = pd.read_parquet(f"{directory}/update_1")
    expected_df = pd.read_parquet(f"{directory}/update_1")
    assert_df_equality(expected_df,actual_df)
    
def test_scd2_simple_insert():
    actual_df = pd.read_parquet(f"{directory}/update_2")
    expected_df = pd.read_parquet(f"{directory}/update_2")
    assert_df_equality(expected_df,actual_df)
    
def test_scd2_complex():
    actual_df = pd.read_parquet(f"{directory}/update_3")
    expected_df = pd.read_parquet(f"{directory}/update_3")
    assert_df_equality(expected_df,actual_df)