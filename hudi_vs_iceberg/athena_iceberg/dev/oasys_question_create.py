

#    DROP TABLE oasys_dev.wto_ice_oasys_question;  
#    --10:05 - this also deletes the underlying data from 
#    s3://mojap-oasys-dev/wto_iceberg/tables/

sql  = """
CREATE TABLE oasys_dev.wto_ice_oasys_question 
WITH (table_type='ICEBERG',
location='s3://mojap-oasys-dev/wto_iceberg/tables/OASYS_QUESTION/',  
format='PARQUET',
is_external=false)
AS 
SELECT op, --string COMMENT 'Type of change, for rows added by ongoing replication.', 
  cast(extraction_timestamp  as timestamp(6)) as extraction_timestamp, --timestamp COMMENT 'DMS extraction timestamp', 
  scn, -- string COMMENT 'Oracle system change number', 
  oasys_question_pk, -- decimal(38,10) COMMENT '', 
  additional_note, -- string COMMENT '', 
  oasys_section_pk, -- decimal(38,10) COMMENT '', 
  free_format_answer, -- string COMMENT '', 
  display_score, -- decimal(38,10) COMMENT '', 
  ref_ass_version_code, -- string COMMENT '', 
  version_number, -- string COMMENT '', 
  ref_question_code, -- string COMMENT '', 
  disclosed_ind, -- string COMMENT '', 
  currently_hidden_ind, -- string COMMENT '', 
  mig_guid, -- string COMMENT '', 
  mig_id, -- string COMMENT '', 
  checksum, -- string COMMENT '', 
  cast(create_date as timestamp(6)) as create_date, -- timestamp COMMENT '', 
  create_user, -- string COMMENT '', 
  cast(lastupd_date as timestamp(6)) as lastupd_date, -- timestamp COMMENT '', 
  lastupd_user, -- string COMMENT '', 
  mojap_current_record, -- boolean COMMENT 'If the record is current', 
  cast(mojap_start_datetime as timestamp(6)) as mojap_start_datetime, -- timestamp COMMENT 'When the record became current', 
  cast(mojap_end_datetime as timestamp(6)) as mojap_end_datetime, -- timestamp COMMENT 'When the record ceased to be current', 
  ref_section_code --, -- string COMMENT '')
FROM oasys_dev_raw_hist.oasys_question;
"""

# -- Run time: 2 min 1.301 sec, Data scanned: 122.73 GB
