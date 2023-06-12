import moto
import pytest
import athena_functions_glue as af

compute = "athena_iceberg"
use_case = "bulk_insert"
bucket = "sb-test-bucket-ireland"
output_key_base = "data-engineering-use-cases"
table = "store_sales"
primary_key = "pk"
scd2_type = "simple"
scale = 1
proportion = 0.001
str_proportion = str(proportion).replace(".", "_")


full_load_path = f"s3://{bucket}/tcpds/scale={scale}/table={table}"
updates_filepath = f"s3://{bucket}/tcpds_updates/scale={scale}/table={table}/proportion={proportion}/"
output_data_directory = f"s3://{bucket}/{output_key_base}/compute={compute}/"
output_data_key = f"{output_key_base}/compute={compute}/"

source_database_name = f"tpcds_{scale}"

def test_scd2_simple():

    compute = "athena_iceberg"
    use_case = "bulk_insert"
    bucket = "sb-test-bucket-ireland"
    output_key_base = "data-engineering-use-cases"
    table = "store_sales"
    primary_key = "pk"
    scd2_type = "simple"
    scale = 1
    proportion = 0.001
    str_proportion = str(proportion).replace(".", "_")


    full_load_path = f"s3://{bucket}/tcpds/scale={scale}/table={table}"
    updates_filepath = f"s3://{bucket}/tcpds_updates/scale={scale}/table={table}/proportion={proportion}/"
    output_data_directory = f"s3://{bucket}/{output_key_base}/compute={compute}/"
    output_data_key = f"{output_key_base}/compute={compute}/"

    source_database_name = f"tpcds_{scale}"


    #Updated variables
    workgroup = "Athena3"
    source_table_name = table
    dest_database_name = f"tpcds_{scale}_{compute}"
    dest_ice_table_name = f"{table}_{scale}_{str_proportion}_{scd2_type}"
    update_table_name = f"{table}_{str_proportion}"
    complex_temp_tbl_name = f"{dest_ice_table_name}_TEMP"
    table_dest_s3_path = f"{output_data_directory}{dest_database_name}/{dest_ice_table_name}" 
    
    # db suggested - datetime.datetime(2250, 1, 1), but cant be botehred testing ICEBERG CAST
    future_end_datetime = '2250-01-01' 

    af.scd2_simple(dest_database_name, dest_ice_table_name, table_dest_s3_path, future_end_datetime, source_database_name, source_table_name)



