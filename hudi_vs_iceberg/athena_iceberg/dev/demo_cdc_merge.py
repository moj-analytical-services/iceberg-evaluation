import awswrangler as wr
import time 

partition = "2023-05-08"
raw_db = "wto_raw_demo"
curated_db = "wto_curated_demo"
table_name = "sporting_event"

sql1 = f"""
SELECT op,
    cdc_timestamp,
    id,
    sport_type_name,
    home_team_id,
    away_team_id,
    location_id,
    start_date_time,
    start_date,
    sold_out
    FROM wto_raw_demo.sporting_event_cdc
    WHERE partition_date ='{partition}'
    """
sql2 = f"""SELECT COUNT(*)
    FROM wto_curated_demo.{table_name}
    """


athena_merge_partition = f"""
    MERGE INTO wto_curated_demo.sporting_event t
    USING (SELECT op,
    cdc_timestamp,
    id,
    sport_type_name,
    home_team_id,
    away_team_id,
    location_id,
    start_date_time,
    start_date,
    sold_out
    FROM wto_raw_demo.sporting_event_cdc
    WHERE partition_date ='{partition}') s
    ON t.id = s.id
    WHEN MATCHED AND s.op = 'D' THEN DELETE
    WHEN MATCHED THEN
    UPDATE SET
    sport_type_name = s.sport_type_name,
    home_team_id = s.home_team_id,
    location_id = s.location_id,
    start_date_time = s.start_date_time,
    start_date = s.start_date,
    sold_out = s.sold_out
    WHEN NOT MATCHED THEN
    INSERT (id,
    sport_type_name,
    home_team_id,
    away_team_id,
    location_id,
    start_date_time,
    start_date)
    VALUES
    (s.id,
    s.sport_type_name,
    s.home_team_id,
    s.away_team_id,
    s.location_id,
    s.start_date_time,
    s.start_date)
"""


print(sql2)

df = wr.athena.read_sql_query(sql=sql2, database="wto_raw_demo", ctas_approach=False)
#, workgroup="Athena3"
print(f"{table_name} contains {df.values[0][0]} records")

merge_into_query_id = wr.athena.start_query_execution(
                    sql=athena_merge_partition, database=curated_db, workgroup='Athena3')


query_status_state = "RUNNING"
cnt = 0
while query_status_state == "RUNNING":
    print("Athena query running {cnt} seconds")
    time.sleep(2)
    query_exec = wr.athena.get_query_execution(query_execution_id=merge_into_query_id)
    query_status_state = query_exec['Status']['State']
    cnt += 2
for key, value in query_exec.items():
    if key not in ['Query']:
        print(key, value)



df = wr.athena.read_sql_query(sql=sql2, database="wto_curated_demo", ctas_approach=False)
#, workgroup="Athena3"
print(f"{table_name} contains {df.values[0][0]} records")