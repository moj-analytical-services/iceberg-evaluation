import awswrangler as wr
import boto3
import json
import logging
import sys

root = logging.getLogger()
root.setLevel(logging.INFO)
logging.getLogger("botocore").setLevel(logging.WARN)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

workgroup = 'Athena3'

def get_job_run_id():
    job_run_id = f"{use_case}_{scd2_type}_{table}_{scale}_{str_proportion}"
    return job_run_id

def run_queries(queries, workgroup, bucket=None, table_dest_s3_key=None):
    results = []
    for query in queries:
        root.info(f"run_queries: Running query: {query}")
        resp = wr.athena.start_query_execution(query, workgroup=workgroup, wait=True)
        results.append(resp)
        if resp['Status']['State'] == 'SUCCEEDED':
            if "DROP TABLE" in query and "_TEMP" not in query:
                root.info(f"run_queries: Sucessfully run DROP ICE DEST TABLE script, now checking folder to delete any residual keys")
                clean_dest_folder(bucket, table_dest_s3_key)
            #time.sleep(0.5)
            continue
        else:
            root.error(f"run_queries: Failed to run query: {query}")
            return 1
    return results

def resp_to_s3(resp, job_run_id):
    root.info("""resp_to_s3: writing Athena response to s3 bucket.""")
    client = boto3.client('s3')
    client.put_object(
            Bucket=bucket, 
            #Key=f'{output_data_key}responses/{job_run_id}.json',
            Key=f"data-engineering-use-cases/compute=athena_iceberg/responses/{job_run_id}.json",
            Body=json.dumps(resp, indent=2, default=str))

def clean_dest_folder(bucket, table_dest_s3_key):
    s3 = boto3.resource("s3")
    boto_bucket = s3.Bucket(bucket)
    objs = boto_bucket.objects.filter(Prefix=table_dest_s3_key)
    root.info(f"""clean_dest_folder: Deleting the following object keys from {bucket}:
    #    {[obj.key for obj in objs]}""")
    objs.delete()

def bulk_insert(bucket, dest_database_name, dest_ice_table_name, table_dest_s3_path, future_end_datetime, source_database_name, source_table_name):
    table_dest_s3_key = table_dest_s3_path.replace(f's3://{bucket}/','')
    create_db_sql = f"CREATE DATABASE IF NOT EXISTS {dest_database_name};"
    drop_dest_sql = f"DROP TABLE IF EXISTS {dest_database_name}.{dest_ice_table_name};"
    bulk_insert_sql = f"""
        CREATE TABLE IF NOT EXISTS {dest_database_name}.{dest_ice_table_name}
        WITH (table_type='ICEBERG',
            location='{table_dest_s3_path}',
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
    queries = [create_db_sql, drop_dest_sql, bulk_insert_sql]
    results = run_queries(queries, workgroup, bucket, table_dest_s3_key)
    return results

def scd2_simple(dest_database_name, dest_ice_table_name, source_database_name, update_table_name, primary_key, future_end_datetime):
    simple_merge_sql = f"""
    MERGE INTO {dest_database_name}.{dest_ice_table_name} dest
    USING (   
    SELECT
            ss_sold_date_sk,
            ss_sold_time_sk,
            ss_item_sk,
            ss_customer_sk,
            ss_cdemo_sk,
            ss_hdemo_sk,
            ss_addr_sk, 
            ss_store_sk,
            ss_promo_sk,
            ss_ticket_number,
            ss_quantity,
            ss_wholesale_cost,
            ss_list_price,
            ss_sales_price, 
            ss_ext_discount_amt,
            ss_ext_sales_price,
            ss_ext_wholesale_cost,
            ss_ext_list_price,
            ss_ext_tax,
            ss_coupon_amt,
            ss_net_paid, 
            ss_net_paid_inc_tax,
            ss_net_profit, 
            extraction_timestamp, 
            op, 
            pk,
            CAST(extraction_timestamp AS TIMESTAMP(6)) AS start_datetime, 
            CAST(TIMESTAMP '2250-01-01' AS TIMESTAMP(6)) AS end_datetime, 
            CAST(true AS boolean) AS is_current
        FROM {source_database_name}.{update_table_name}
    UNION ALL
    SELECT
            t.ss_sold_date_sk,
            t.ss_sold_time_sk, 
            t.ss_item_sk,
            t.ss_customer_sk,
            t.ss_cdemo_sk,
            t.ss_hdemo_sk,
            t.ss_addr_sk, 
            t.ss_store_sk,
            t.ss_promo_sk,
            t.ss_ticket_number,
            t.ss_quantity, 
            t.ss_wholesale_cost,
            t.ss_list_price,
            t.ss_sales_price, 
            t.ss_ext_discount_amt,
            t.ss_ext_sales_price,
            t.ss_ext_wholesale_cost, 
            t.ss_ext_list_price,
            t.ss_ext_tax,
            t.ss_coupon_amt,
            t.ss_net_paid, 
            t.ss_net_paid_inc_tax,
            t.ss_net_profit, 
            t.extraction_timestamp, 
            t.op, 
            t.pk,
            t.start_datetime,
            u.extraction_timestamp AS end_datetime, --u.start_datetime AS end_datetime, 
            false --u.is_current
    FROM {dest_database_name}.{dest_ice_table_name} t
    INNER JOIN {source_database_name}.{update_table_name} u
        ON t.pk = u.pk 
        AND t.is_current = true
    ) AS src
    ON (dest.pk = src.pk 
        AND dest.extraction_timestamp = src.extraction_timestamp)
        WHEN MATCHED THEN
            UPDATE SET end_datetime = src.end_datetime, is_current = false
        WHEN NOT MATCHED THEN 
            INSERT (
                ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, 
                ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, 
                ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, 
                ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, extraction_timestamp, op, pk, start_datetime, end_datetime, is_current
            )
            VALUES (
                src.ss_sold_date_sk, src.ss_sold_time_sk, src.ss_item_sk, src.ss_customer_sk, src.ss_cdemo_sk, src.ss_hdemo_sk, 
                src.ss_addr_sk, src.ss_store_sk, src.ss_promo_sk, src.ss_ticket_number, src.ss_quantity, src.ss_wholesale_cost,
                src.ss_list_price, src.ss_sales_price, src.ss_ext_discount_amt, src.ss_ext_sales_price, src.ss_ext_wholesale_cost, 
                src.ss_ext_list_price, src.ss_ext_tax, src.ss_coupon_amt, src.ss_net_paid, src.ss_net_paid_inc_tax, src.ss_net_profit, 
                src.extraction_timestamp, src.op, src.pk, src.extraction_timestamp, src.end_datetime, true
            )
    """
    queries = [simple_merge_sql]
    results = run_queries(queries, workgroup)
    return results
    
def scd2_complex(dest_database_name, dest_ice_table_name, source_database_name, update_table_name, primary_key, future_end_datetime):
    complex_merge_sql = f"""
    MERGE INTO {dest_database_name}.{dest_ice_table_name} dest
    USING (
        WITH t1 AS (
            --## ALL NEW CDC RECORDS 
            SELECT
                ss_sold_date_sk,
                ss_sold_time_sk,
                ss_item_sk,
                ss_customer_sk,
                ss_cdemo_sk,
                ss_hdemo_sk,
                ss_addr_sk, 
                ss_store_sk,
                ss_promo_sk,
                ss_ticket_number,
                ss_quantity,
                ss_wholesale_cost,
                ss_list_price,
                ss_sales_price, 
                ss_ext_discount_amt,
                ss_ext_sales_price,
                ss_ext_wholesale_cost,
                ss_ext_list_price,
                ss_ext_tax,
                ss_coupon_amt,
                ss_net_paid, 
                ss_net_paid_inc_tax,
                ss_net_profit, 
                extraction_timestamp, 
                op, 
                pk,
                extraction_timestamp AS start_datetime
            FROM {source_database_name}.{update_table_name} --## CDC INCOMING TBL
            UNION ALL
                    --## ALL CURRENT DATA WITH MATCHING PK
            SELECT
                t.ss_sold_date_sk,
                t.ss_sold_time_sk, 
                t.ss_item_sk,
                t.ss_customer_sk,
                t.ss_cdemo_sk,
                t.ss_hdemo_sk,
                t.ss_addr_sk, 
                t.ss_store_sk,
                t.ss_promo_sk,
                t.ss_ticket_number,
                t.ss_quantity, 
                t.ss_wholesale_cost,
                t.ss_list_price,
                t.ss_sales_price, 
                t.ss_ext_discount_amt,
                t.ss_ext_sales_price,
                t.ss_ext_wholesale_cost, 
                t.ss_ext_list_price,
                t.ss_ext_tax,
                t.ss_coupon_amt,
                t.ss_net_paid, 
                t.ss_net_paid_inc_tax,
                t.ss_net_profit, 
                t.extraction_timestamp, 
                t.op, 
                t.pk,
                t.start_datetime
            FROM {dest_database_name}.{dest_ice_table_name} as t --##DEST ICEBERG TABLE
            INNER JOIN {source_database_name}.{update_table_name} as u  --##INCOMING CDC TABLE
                ON t.pk = u.pk
        ), 
        t2 AS (
            SELECT *,
                    LEAD(extraction_timestamp,1,TIMESTAMP '{future_end_datetime}' )
                    OVER(PARTITION BY pk ORDER BY extraction_timestamp) AS end_datetime
            FROM t1
        )
        SELECT *,
            (CASE WHEN end_datetime= TIMESTAMP '{future_end_datetime}'  THEN true ELSE false END) AS is_current
        FROM t2
            ORDER BY pk, extraction_timestamp
                --SELECT * FROM src LIMIT 10
    ) AS src
--## t1 = UNION OF INCOMING CDC + EXISTING RECORDS WITH SAME PK's
--## t2 = WINDOWING OVER ALL RECORDS IN T1 TO GET CORRECT END DATE / IS CURRENT
--## scd2_rows = ln 75 - 78 = t1 + end_date from t2 

        ON (dest.pk = src.pk AND dest.extraction_timestamp=src.extraction_timestamp)
        WHEN MATCHED THEN
            UPDATE SET end_datetime = CAST(src.end_datetime AS TIMESTAMP(6))
            ,is_current = src.is_current
        WHEN NOT MATCHED THEN 
            INSERT (
                ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, 
                ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, 
                ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, 
                ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, extraction_timestamp, op, pk, start_datetime, end_datetime, is_current
            )
            VALUES (
                src.ss_sold_date_sk, src.ss_sold_time_sk, src.ss_item_sk, src.ss_customer_sk, src.ss_cdemo_sk, src.ss_hdemo_sk, 
                src.ss_addr_sk, src.ss_store_sk, src.ss_promo_sk, src.ss_ticket_number, src.ss_quantity, src.ss_wholesale_cost,
                src.ss_list_price, src.ss_sales_price, src.ss_ext_discount_amt, src.ss_ext_sales_price, src.ss_ext_wholesale_cost, 
                src.ss_ext_list_price, src.ss_ext_tax, src.ss_coupon_amt, src.ss_net_paid, src.ss_net_paid_inc_tax, src.ss_net_profit,
                CAST(src.extraction_timestamp AS TIMESTAMP(6)), src.op, src.pk, CAST(src.extraction_timestamp AS TIMESTAMP(6)), 
                CAST(src.end_datetime AS TIMESTAMP(6)), src.is_current
            )
"""
    queries = [complex_merge_sql]
    results = run_queries(queries, workgroup)
    return results

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
    table_dest_s3_path = f"{output_data_directory}{dest_database_name}/{dest_ice_table_name}" 
    job_run_id = f"{use_case}_{scd2_type}_{table}_{scale}_{str_proportion}"

    # db suggested - datetime.datetime(2250, 1, 1), but cant be botehred testing ICEBERG CAST
    future_end_datetime = '2250-01-01' 
    
    
    if use_case == "bulk_insert":
        results = bulk_insert(bucket, dest_database_name, dest_ice_table_name, table_dest_s3_path, future_end_datetime, source_database_name, source_table_name)
        resp_to_s3(results, get_job_run_id())

    if use_case == "scd2_simple":
        results = scd2_simple(dest_database_name, dest_ice_table_name, source_database_name, update_table_name, primary_key, future_end_datetime)
        resp_to_s3(results, get_job_run_id())

    if use_case == "scd2_complex":
        results = scd2_complex(dest_database_name, dest_ice_table_name, source_database_name, update_table_name, complex_temp_tbl_name, output_data_directory)
        resp_to_s3(results, get_job_run_id())
