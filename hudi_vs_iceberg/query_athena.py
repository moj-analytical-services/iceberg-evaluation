import awswrangler as wr

partition = "2023-05-08"
sql = """
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

print(sql)

df = wr.athena.read_sql_query(sql=sql, database="wto_raw_demo", ctas_approach=False, workgroup="Athena3")

print(df)