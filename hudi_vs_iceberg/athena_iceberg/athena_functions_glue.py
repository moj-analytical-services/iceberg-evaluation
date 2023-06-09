import awswrangler as wr
import boto3
import json
import logging
import sys
import time

root = logging.getLogger()
root.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)
root.info("check")

# settings and boilerplate code can go here (if its the same for all use cases) 
def get_job_run_id():
    job_run_id = f"{use_case}_{scd2_type}_{table}_{scale}_{str_proportion}"
    return job_run_id

def resp_to_s3(resp, job_run_id):
    client = boto3.client('s3')
    client.put_object(
            Bucket=bucket, 
            #Key=f'{output_data_key}responses/{job_run_id}.json',
            Key=f"data-engineering-use-cases/compute=athena_iceberg/responses/{job_run_id}.json",
            Body=json.dumps(resp, indent=2, default=str))

def run_queries(queries, workgroup):
    results = []
    for query in queries:
        resp = wr.athena.start_query_execution(query, workgroup=workgroup, wait=True)
        results.append(resp)
        if resp['Status']['State'] == 'SUCCEEDED':
            #time.sleep(0.5)
            continue
        else:
            return 1
    return results
    
def clean_dest_folder(bucket, table_dest_folder):
    client = boto3.client('s3')
    obj_list_resp = client.list_objects_v2(Bucket=bucket, Prefix=table_dest_folder)
    root.info(f"""Object list from destination table folder {table_dest_folder}:
        {obj_list_resp}""")
    for obj in obj_list_resp['Contents']:
        client.delete_object(Bucket=bucket, Key=obj['Key'])
        root.info(f"Deleted {obj['Key']}")

def bulk_insert(bucket, dest_database_name, dest_ice_table_name, output_data_directory, future_end_datetime, source_database_name, source_table_name):
    table_dest_folder = f"{output_data_directory}{dest_database_name}/{dest_ice_table_name}"
    root.info(f"""Vars recieved to bulk insert:
        bucket: {bucket}
        dest_database_name: {dest_database_name}
        dest_ice_table_name: {dest_ice_table_name}
        output_data_directory: {output_data_directory}
        future_end_datetime: {future_end_datetime}
        source_database_name: {source_database_name}
        source_table_name: {source_table_name}
        table_dest_folder: {table_dest_folder}
        """)
    create_db_sql = f"CREATE DATABASE IF NOT EXISTS {dest_database_name};"
    drop_dest_sql = f"DROP TABLE IF EXISTS {dest_database_name}.{dest_ice_table_name} PURGE;"
    bulk_insert_sql = f"""
        CREATE TABLE IF NOT EXISTS {dest_database_name}.{dest_ice_table_name}
        WITH (table_type='ICEBERG',
            location='{table_dest_folder}',
            format='PARQUET',
            is_external=false)
        AS SELECT
            ss_sold_date_sk     
            ,ss_sold_time_sk     
            ,ss_item_sk          
            ,ss_customer_sk      
            ,ss_cdemo_sk         
            ,ss_hdemo_sk         
            ,ss_addr_sk          
            ,ss_store_sk         
            ,ss_promo_sk         
            ,ss_ticket_number    
            ,ss_quantity         
            ,ss_wholesale_cost   
            ,ss_list_price       
            ,ss_sales_price      
            ,ss_ext_discount_amt 
            ,ss_ext_sales_price  
            ,ss_ext_wholesale_cost
            ,ss_ext_list_price   
            ,ss_ext_tax          
            ,ss_coupon_amt       
            ,ss_net_paid         
            ,ss_net_paid_inc_tax 
            ,ss_net_profit       
            ,CAST(extraction_timestamp  as timestamp(6)) as extraction_timestamp
            ,op                  
            ,pk  
            ,CAST(extraction_timestamp AS TIMESTAMP(6)) AS start_datetime
            ,CAST('{future_end_datetime}' AS TIMESTAMP(6)) AS end_datetime
            ,CAST(true AS boolean) AS is_current
        FROM {source_database_name}.{source_table_name} src;
    """
    job_run_id = get_job_run_id()
    clean_dest_folder(bucket, table_dest_folder)
    queries = [create_db_sql, drop_dest_sql, bulk_insert_sql]
    results = run_queries(queries, workgroup)
    resp_to_s3(results, job_run_id)

def scd2_simple(dest_database_name, dest_ice_table_name, source_database_name, update_table_name, primary_key, future_end_datetime):
    simple_insert_sql = f"""
        INSERT INTO {dest_database_name}.{dest_ice_table_name}
            SELECT
            ss_sold_date_sk     
            ,ss_sold_time_sk     
            ,ss_item_sk          
            ,ss_customer_sk      
            ,ss_cdemo_sk         
            ,ss_hdemo_sk         
            ,ss_addr_sk          
            ,ss_store_sk         
            ,ss_promo_sk         
            ,ss_ticket_number    
            ,ss_quantity         
            ,ss_wholesale_cost   
            ,ss_list_price       
            ,ss_sales_price      
            ,ss_ext_discount_amt 
            ,ss_ext_sales_price  
            ,ss_ext_wholesale_cost
            ,ss_ext_list_price   
            ,ss_ext_tax          
            ,ss_coupon_amt       
            ,ss_net_paid         
            ,ss_net_paid_inc_tax 
            ,ss_net_profit       
            ,CAST(extraction_timestamp  as timestamp(6)) as extraction_timestamp
            ,op                  
            ,pk  
            ,CAST(extraction_timestamp AS TIMESTAMP(6)) AS start_datetime
            ,NULL
            ,NULL
            FROM {source_database_name}.{update_table_name} src;
    """
    simple_merge_sql = f"""
        MERGE INTO {dest_database_name}.{dest_ice_table_name} dest
            USING {source_database_name}.{update_table_name} src
            ON src.{primary_key} = dest.{primary_key}
            WHEN MATCHED AND dest.is_current = TRUE 
                THEN UPDATE
                    SET end_datetime = src.extraction_timestamp, is_current = FALSE
            WHEN MATCHED AND dest.extraction_timestamp = dest.start_datetime
                THEN UPDATE
                SET end_datetime = CAST(TIMESTAMP '{future_end_datetime}' AS TIMESTAMP(6))
                , is_current = TRUE;
    """
    job_run_id = get_job_run_id()
    queries = [simple_insert_sql, simple_merge_sql]
    results = run_queries(queries, workgroup)
    resp_to_s3(results, job_run_id)
    
def scd2_complex(dest_database_name, dest_ice_table_name, source_database_name, update_table_name, complex_temp_tbl_name, output_data_directory):
    simple_insert_sql = f"""
        INSERT INTO {dest_database_name}.{dest_ice_table_name}
            SELECT
            ss_sold_date_sk     
            ,ss_sold_time_sk     
            ,ss_item_sk          
            ,ss_customer_sk      
            ,ss_cdemo_sk         
            ,ss_hdemo_sk         
            ,ss_addr_sk          
            ,ss_store_sk         
            ,ss_promo_sk         
            ,ss_ticket_number    
            ,ss_quantity         
            ,ss_wholesale_cost   
            ,ss_list_price       
            ,ss_sales_price      
            ,ss_ext_discount_amt 
            ,ss_ext_sales_price  
            ,ss_ext_wholesale_cost
            ,ss_ext_list_price   
            ,ss_ext_tax          
            ,ss_coupon_amt       
            ,ss_net_paid         
            ,ss_net_paid_inc_tax 
            ,ss_net_profit       
            ,CAST(extraction_timestamp  AS TIMESTAMP(6)) as extraction_timestamp
            ,op                  
            ,pk  
            ,CAST(extraction_timestamp AS TIMESTAMP(6)) AS start_datetime
            ,NULL
            ,NULL
            FROM {source_database_name}.{update_table_name} src;
    """
    drop_complex_temp_tbl_sql = f"DROP TABLE IF EXISTS {dest_database_name}.{complex_temp_tbl_name}"
    create_complex_temp_tbl_sql = f"""
        CREATE TABLE IF NOT EXISTS {dest_database_name}.{complex_temp_tbl_name}
        WITH (table_type='ICEBERG',
        location='{output_data_directory}{dest_database_name}/{complex_temp_tbl_name}',
        format='PARQUET',
        is_external=false)
        AS 
        WITH end_date AS (
            SELECT {primary_key}, CAST(extraction_timestamp  AS TIMESTAMP(6)) as extraction_timestamp, 
                    LEAD(CAST(extraction_timestamp  AS TIMESTAMP(6)), 1, TIMESTAMP '{future_end_datetime}')
                OVER (PARTITION BY {primary_key} 
            ORDER BY extraction_timestamp) AS end_datetime_lead
            FROM {source_database_name}.{update_table_name}
        )
        SELECT {primary_key}, extraction_timestamp, end_datetime_lead,
            CASE WHEN end_datetime_lead = CAST(TIMESTAMP '{future_end_datetime}' AS TIMESTAMP(6)) THEN true
                ELSE false END AS is_current 
        FROM end_date;
    """
    complex_merge_sql = f"""
        MERGE INTO {dest_database_name}.{dest_ice_table_name} ice
            USING {dest_database_name}.{complex_temp_tbl_name} tmp
            ON (tmp.{primary_key} = ice.{primary_key}
            AND tmp.extraction_timestamp = ice.extraction_timestamp)
            WHEN MATCHED 
                THEN UPDATE
                    SET end_datetime = tmp.end_datetime_lead,
                        is_current = tmp.is_current;
    """
    drop_dest_sql = f"DROP TABLE IF EXISTS {dest_database_name}.{dest_ice_table_name} PURGE;"
    job_run_id = get_job_run_id()
    queries = [simple_insert_sql, drop_complex_temp_tbl_sql, create_complex_temp_tbl_sql, complex_merge_sql, drop_complex_temp_tbl_sql, drop_dest_sql]
    results = run_queries(queries, workgroup)
    resp_to_s3(results, job_run_id)
    
if __name__ == "__main__":
    import sys
    from awsglue.utils import getResolvedOptions

    
    # these are the arguments passed to the glue-job from step functions
    # you dont need to include them if you dont want
    args = getResolvedOptions(sys.argv, ["use_case",
                                         "bucket",
                                         "output_key_base",
                                         "table",
                                         "primary_key",
                                         "scale",
                                         "proportion",
                                         "scd2_type"
                                         ])
    root.info(f"args:{str(args)}")
    root.info(f"sys.argv: {str(sys.argv)}")
    compute = "athena_iceberg"
    use_case = args.get("use_case")
    bucket = args.get("bucket")
    output_key_base = args.get("output_key_base")
    table = args.get("table")
    primary_key = args.get("primary_key")
    scd2_type = args.get("scd2_type")
    scale = args.get("scale")
    proportion = args.get("proportion")
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
    
    # db suggested - datetime.datetime(2250, 1, 1), but cant be botehred testing ICEBERG CAST
    future_end_datetime = '2250-01-01' 
    
    
    if use_case == "bulk_insert":
        _ = bulk_insert(bucket, dest_database_name, dest_ice_table_name, output_data_directory, future_end_datetime, source_database_name, source_table_name)
    
    if use_case == "scd2_simple":
        _ = scd2_simple(dest_database_name, dest_ice_table_name, source_database_name, update_table_name, primary_key, future_end_datetime)
    
    if use_case == "scd2_complex":
        _ = scd2_complex(dest_database_name, dest_ice_table_name, source_database_name, update_table_name, complex_temp_tbl_name, output_data_directory)
