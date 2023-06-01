import awswrangler as wr
import time

db_name = "wto_hudi_iceberg"

bulk_insert_drop_sql = f"""
    DROP TABLE IF EXISTS {db_name}.{{dest_table_name}};
"""

bulk_insert_sql = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{{dest_table_name}}
        WITH (table_type='ICEBERG',
        location='{{db_base_path}}',
        format='PARQUET',
        is_external=false)
        AS SELECT
            src.product_id, src.product_name, src.price, 
            CAST(extraction_timestamp AS TIMESTAMP(6)) AS extraction_timestamp, src.op,
            CAST(extraction_timestamp AS TIMESTAMP(6)) AS start_datetime,
            CAST(TIMESTAMP '{{future_end_datetime}}' AS TIMESTAMP(6)) AS end_datetime,
            CAST(true AS boolean) AS is_current
           FROM {db_name}.{{source_table_name}} src;
"""

simple_insert = f"""
    INSERT INTO {db_name}.{{dest_table_name}}
        SELECT src.product_id, src.product_name, src.price, 
            CAST(src.extraction_timestamp AS TIMESTAMP(6)) AS extraction_timestamp, src.op,
            CAST(src.extraction_timestamp AS TIMESTAMP(6)), NULL, NULL
        FROM {db_name}.{{source_table_name}} src
"""
#, CAST(TIMESTAMP '{{future_end_datetime}}' as TIMESTAMP(6)),TRUE
simple_merge = f"""
    MERGE INTO {db_name}.{{dest_table_name}} dest
    USING {db_name}.{{source_table_name}} src
        ON src.{{primary_key}} = dest.{{primary_key}}
    WHEN MATCHED AND dest.is_current = TRUE 
        THEN UPDATE
            SET end_datetime = src.extraction_timestamp, is_current = FALSE
    WHEN MATCHED AND dest.extraction_timestamp = dest.start_datetime
        THEN UPDATE
        SET end_datetime = CAST(TIMESTAMP '{{future_end_datetime}}' AS TIMESTAMP(6))
        , is_current = TRUE;
"""

complex_temp_table = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{{temp_table_name}}
        WITH (table_type='ICEBERG',
        location='{{temp_db_base_path}}',
        format='PARQUET',
        is_external=false)
    AS 
    WITH end_date AS (
        SELECT {{primary_key}}, extraction_timestamp, LEAD(extraction_timestamp, 1, TIMESTAMP '{{future_end_datetime}}')
            OVER (PARTITION BY {{primary_key}} 
        ORDER BY extraction_timestamp) AS end_datetime_lead
        FROM {db_name}.{{dest_table_name}}
    )
    SELECT {{primary_key}}, extraction_timestamp, end_datetime_lead,
        CASE WHEN end_datetime_lead = CAST(TIMESTAMP '{{future_end_datetime}}' AS TIMESTAMP(6)) THEN true
            ELSE false END AS is_current 
    FROM end_date

"""

complex_merge = f"""
    MERGE INTO {db_name}.{{dest_table_name}} ice
    USING {db_name}.{{temp_table_name}} tmp
    ON (tmp.{{primary_key}} = ice.{{primary_key}}
    AND tmp.extraction_timestamp = ice.extraction_timestamp)
    WHEN MATCHED 
        THEN UPDATE
            SET end_datetime = tmp.end_datetime_lead,
                is_current = tmp.is_current
"""

complex_temp_drop = f"""
    DROP TABLE IF EXISTS {db_name}.{{temp_table_name}};
"""
# WHERE partition_date='2023-05-09'

def bulk_insert(input_filepath, output_directory, future_end_datetime, 
                bulk_insert_sql=bulk_insert_sql,
                bulk_insert_drop_sql=bulk_insert_drop_sql, 
                demo=""):
    start_time = time.time()
    current_test = input_filepath.split("/")[-3].replace("-", "_")
    dest_table_name = f"ICE_{current_test}"
    db_base_path = f"{output_directory}database/{dest_table_name}"
    source_table_name = f"{current_test}_full_load"
    ## Update SQL statement  with variables from function
    bulk_insert_sql = bulk_insert_sql.replace("{dest_table_name}", dest_table_name)
    bulk_insert_sql = bulk_insert_sql.replace("{source_table_name}", source_table_name)
    bulk_insert_sql = bulk_insert_sql.replace("{db_base_path}", db_base_path)
    bulk_insert_sql = bulk_insert_sql.replace("{future_end_datetime}", str(future_end_datetime))

    bulk_insert_drop_sql = bulk_insert_drop_sql.replace("{dest_table_name}", dest_table_name)
    ## Create a demo table when running on Jupyter Notebook
    if demo == "print_sql":
        return f"""
        **DROP DEST TABLE** \n    {bulk_insert_drop_sql}
        **CREATE DEST TABLE** \n    {bulk_insert_sql}
        """
    ## Return SQL statement when running on in step function
    else:
        bulk_milli_sec = []
        bulk_bytes_scanned = []
        resp_bulk_drop = wr.athena.start_query_execution(
            sql=bulk_insert_drop_sql, database=db_name, workgroup='Athena3', wait=True)
        bulk_milli_sec.append(resp_bulk_drop['Statistics']['EngineExecutionTimeInMillis'])
        bulk_bytes_scanned.append(resp_bulk_drop['Statistics']['DataScannedInBytes'])
        print(f"completed DROP in {bulk_milli_sec[0]} milliseconds,  {bulk_bytes_scanned[0]} bytes scanned")
        resp_bulk_insert = wr.athena.start_query_execution(
            sql=bulk_insert_sql, database=db_name, workgroup='Athena3', wait=True)
        bulk_milli_sec.append(resp_bulk_insert['Statistics']['EngineExecutionTimeInMillis'])
        bulk_bytes_scanned.append(resp_bulk_insert['Statistics']['DataScannedInBytes'])
        print(f"completed INSERT in {bulk_milli_sec[1]} milliseconds,  {bulk_bytes_scanned[1]} bytes scanned")
        total_mb_scanned_bulk = sum(bulk_bytes_scanned) / (1024**2)
        print(f"total MB scanned: {total_mb_scanned_bulk} in {sum(bulk_milli_sec)/1000} seconds, ~ Cost =  ${total_mb_scanned_bulk*5/(1024**2)}") 
        
    

def scd2_simple(input_filepath, updates_filepath, 
                output_directory, future_end_datetime, primary_key, 
                simple_insert=simple_insert, 
                simple_merge=simple_merge, demo=""):
    start_time = time.time()
    current_test = input_filepath.split("/")[-3].replace("-", "_")
    dest_table_name = f"ICE_{current_test}"
    db_base_path = f"{output_directory}database/{dest_table_name}"
    source_table_name = f"{current_test}_updates"
    ## Update SQL statement  with variables from function
    simple_insert = simple_insert.replace("{dest_table_name}", dest_table_name)
    simple_insert = simple_insert.replace("{source_table_name}", source_table_name)
    simple_insert = simple_insert.replace("{future_end_datetime}", str(future_end_datetime))
    simple_merge = simple_merge.replace("{dest_table_name}", dest_table_name)
    simple_merge = simple_merge.replace("{source_table_name}", source_table_name)
    simple_merge = simple_merge.replace("{future_end_datetime}", str(future_end_datetime))
    simple_merge = simple_merge.replace("{primary_key}", primary_key)
    if demo == "print_sql":
        return f"**SIMPLE INSERT**\n{simple_insert}\n\n**SIMPLE MERGE**\n{simple_merge}"
    else:
        simple_milli_sec = []
        simple_bytes_scanned = []
        resp_simple_insert = wr.athena.start_query_execution(
            sql=simple_insert, database=db_name, workgroup='Athena3', wait=True)
        simple_milli_sec.append(resp_simple_insert['Statistics']['EngineExecutionTimeInMillis'])
        simple_bytes_scanned.append(resp_simple_insert['Statistics']['DataScannedInBytes'])        
        print(f"completed INSERT in {simple_milli_sec[0]} milliseconds,  {simple_bytes_scanned[0]} bytes scanned")
        wr.athena.start_query_execution(
            sql=simple_merge, database=db_name, workgroup='Athena3', wait=True)
        simple_milli_sec.append(resp_simple_insert['Statistics']['EngineExecutionTimeInMillis'])
        simple_bytes_scanned.append(resp_simple_insert['Statistics']['DataScannedInBytes'])
        print(f"completed MERGE in {simple_milli_sec[1]} milliseconds,  {simple_bytes_scanned[1]} bytes scanned")
        total_mb_scanned_simple = sum(simple_bytes_scanned) / (1024**2)
        print(f"total MB scanned: {total_mb_scanned_simple} in {sum(simple_milli_sec)/1000} seconds, ~ Cost =  ${total_mb_scanned_simple*5/(1024**2)}")
        return db_base_path
    ## Return SQL statement when running on in step function
        

def scd2_complex(input_filepath, updates_filepath, 
                output_directory, future_end_datetime, primary_key,
                simple_insert=simple_insert,
                complex_temp_table=complex_temp_table,
                complex_merge=complex_merge,
                complex_temp_drop=complex_temp_drop,
                demo=""):
    current_test = input_filepath.split("/")[-3].replace("-", "_")
    dest_table_name = f"ICE_{current_test}"
    temp_table_name = f"{dest_table_name}_temp"
    db_base_path = f"{output_directory}database/{dest_table_name}"
    temp_db_base_path = f"{output_directory}database/{temp_table_name}"
    source_table_name = f"{current_test}_late_updates"
    start_time = time.time()    
    simple_insert = simple_insert.replace("{dest_table_name}", dest_table_name)
    simple_insert = simple_insert.replace("{source_table_name}", source_table_name)
    simple_insert = simple_insert.replace("{future_end_datetime}", str(future_end_datetime))
    
    complex_temp_table = complex_temp_table.replace("{temp_table_name}", temp_table_name)
    complex_temp_table = complex_temp_table.replace("{temp_db_base_path}", temp_db_base_path)
    complex_temp_table = complex_temp_table.replace("{primary_key}", primary_key)
    complex_temp_table = complex_temp_table.replace("{dest_table_name}", dest_table_name)
    complex_temp_table = complex_temp_table.replace("{future_end_datetime}", str(future_end_datetime))
    
    complex_merge = complex_merge.replace("{dest_table_name}", dest_table_name)
    complex_merge = complex_merge.replace("{temp_table_name}", temp_table_name)
    complex_merge = complex_merge.replace("{primary_key}", primary_key)

    complex_temp_drop = complex_temp_drop.replace("{temp_table_name}", temp_table_name)
    if demo == "print_sql":
        return f"""
        **SIMPLE INSERT**\n{simple_insert}
        **COMPLEX TEMP TABLE**\n{complex_temp_table}
        **COMPLEX MERGE**\n{complex_merge}
        **COMPLEX DROP**\n{complex_temp_drop}
        
    """
    ## Return SQL statement when running on in step function
    else:
        milli_sec = []
        bytes_scanned = []
        print("INSERT CDC's ...")
        resp_insert = wr.athena.start_query_execution(
            sql=simple_insert, database=db_name, workgroup='Athena3', wait=True)
        milli_sec.append(resp_insert['Statistics']['TotalExecutionTimeInMillis'])
        bytes_scanned.append(resp_insert['Statistics']['DataScannedInBytes'])
        print(f"""Completed INSERT in {milli_sec[0]} milliseconds, scanned {bytes_scanned[0]} bytes
        CREATE TEMP TABLE ...""")
        resp_temp_table = wr.athena.start_query_execution(
            sql=complex_temp_table, database=db_name, workgroup='Athena3', wait=True)
        milli_sec.append(resp_temp_table['Statistics']['TotalExecutionTimeInMillis'])
        bytes_scanned.append(resp_temp_table['Statistics']['DataScannedInBytes'])
        
        print(f"""Completed CREATE TEMP TABLE in {milli_sec[1]} milliseconds, scanned {bytes_scanned[1]} bytes
        MERGE UPDATES ...""")
        resp_merge = wr.athena.start_query_execution(
            sql=complex_merge, database=db_name, workgroup='Athena3', wait=True)
        milli_sec.append(resp_merge['Statistics']['TotalExecutionTimeInMillis'])
        bytes_scanned.append(resp_merge['Statistics']['DataScannedInBytes'])
        print(f"""Completed MERGE in {milli_sec[2]} milliseconds, scanned {bytes_scanned[2]} bytes
        DROP TEMP TABLE ...""")
        resp_drop = wr.athena.start_query_execution(
            sql=complex_temp_drop, database=db_name, workgroup='Athena3', wait=True)
        milli_sec.append(resp_drop['Statistics']['TotalExecutionTimeInMillis'])
        bytes_scanned.append(resp_drop['Statistics']['DataScannedInBytes'])
        print(f"""Completed DROP TEMP TABLE in {milli_sec[3]} milliseconds, scanned {bytes_scanned[3]} bytes""")
        total_mb_scanned_complex = sum(bytes_scanned)/(1024**2)
        print(f"Total Scanned MB: {total_mb_scanned_complex} in {sum(milli_sec)/1000} seconds, ~ Cost = ${total_mb_scanned_complex*5/(1024**2)}") 
        return db_base_path
