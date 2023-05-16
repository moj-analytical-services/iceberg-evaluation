import awswrangler as wr
from datetime import date, timedelta
import time

raw_bucket = "mojap-raw-hist-dev"
raw_folder_fl = "/hmpps/oasys/EOR/OASYS_QUESTION/"
raw_folder_cdc = "/hmpps/oasys/archive/OASYS_QUESTION/"  #partitioned by day
raw_db = "oasys_dev_raw_hist"
raw_table_name_fl = "oasys_question"
raw_table_name_cdc = "oasys_question_cdc"

curated_bucket = "mojap-oasys-dev"
curated_folder = "/wto_iceberg/tables/OASYS_QUESTION/"
curated_db = "oasys_dev"
table_name = "wto_ice_oasys_question"

create_cdc_table_sql = f"""
CREATE EXTERNAL TABLE oasys_question_cdc_part(
  op string COMMENT 'Type of change, for rows added by ongoing replication.', 
  extraction_timestamp string COMMENT 'DMS extraction timestamp', 
  scn string COMMENT 'Oracle system change number', 
  oasys_question_pk decimal(38,10) COMMENT '', 
  additional_note string COMMENT '', 
  oasys_section_pk decimal(38,10) COMMENT '', 
  free_format_answer string COMMENT '', 
  display_score decimal(38,10) COMMENT '', 
  ref_ass_version_code string COMMENT '', 
  version_number string COMMENT '', 
  ref_question_code string COMMENT '', 
  disclosed_ind string COMMENT '', 
  currently_hidden_ind string COMMENT '', 
  mig_guid string COMMENT '', 
  mig_id string COMMENT '', 
  checksum string COMMENT '', 
  create_date timestamp COMMENT '', 
  create_user string COMMENT '', 
  lastupd_date timestamp COMMENT '', 
  lastupd_user string COMMENT '', 
  mojap_current_record boolean COMMENT 'If the record is current', 
  mojap_start_datetime timestamp COMMENT 'When the record became current', 
  mojap_end_datetime timestamp COMMENT 'When the record ceased to be current', 
  ref_section_code string COMMENT '')
PARTITIONED BY (partition_date date)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
LOCATION
  's3://mojap-raw-hist-dev/hmpps/oasys/archive/OASYS_QUESTION'
TBLPROPERTIES (
  'classification'='parquet')
"""

query_id = wr.athena.start_query_execution(
            sql=create_cdc_table_sql, database=raw_db) #, workgroup='Athena3'


query_status_state = "RUNNING"
cnt = 0
while query_status_state == "RUNNING":
    print(f"Athena query running {cnt} seconds, sleeping for 3 sec")
    time.sleep(3)
    query_exec = wr.athena.get_query_execution(query_execution_id=query_id)
    query_status_state = query_exec['Status']['State']
    cnt += 3

for key, value in query_exec.items():
    if key not in ['Query']:
        print(key, value)


## This code builds up separate partiitions statements for each day in the date range.
## This woud have been required if the source files were iceberg, but because they are 
## in parquet format on hive, they are native to glue and can be updated with MSCK 
## without any additional work.

sdate = date(2021, 5, 19)
edate = date(2023, 3, 10)

date_range = [sdate+timedelta(days=x) for x in range((edate-sdate).days)]

date_partition_sql = ""
for dte in date_range:
    dte_str_part = dte.strftime("%Y-%m-%d")
    dte_str_s3 = dte.strftime("%Y/%m/%d")
    ## In production we would be adding partitions to the raw history cdc table as part of daily cdc processing 
    if len(date_partition_sql) == 0:
        date_partition_sql += f"""
            ALTER TABLE {raw_db}.{raw_table_name_cdc} ADD IF NOT EXISTS PARTITION (partitiondate='{dte_str_part}') LOCATION 's3://{raw_bucket}/{raw_folder_cdc}{dte_str_s3}/'
        """
    else: 
        date_partition_sql += f"""
            \nPARTITION (partitiondate='{dte_str_part}') LOCATION 's3://{raw_bucket}/{raw_folder_cdc}{dte_str_s3}/'
        """

print(f"Data partition SQL query length: {len(date_partition_sql)}")

date_partition_sql = f"MSCK REPAIR TABLE {raw_db}.{raw_table_name_cdc}"

query_id = wr.athena.start_query_execution(
            sql=date_partition_sql, database=raw_db) #, workgroup='Athena3'