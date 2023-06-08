import os

import awswrangler as wr
from jinja2 import Template

os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"

wr.config.workgroup = "AthenaWorkGroupIceberg"

database_name = "tpcds_1"
iceberg_database_name = "tcpds_1_iceberg"

tables = wr.catalog.get_tables(database=database_name)

wr.catalog.create_database(name=iceberg_database_name)

create_sql_template = Template(
    """
    CREATE TABLE {{ database_name }}.{{ table_name }}(
    {% for column in columns %}
        {{ column["Name"] }} {{ column["Type"] }}{{ ", " if not loop.last else "" }}
    {% endfor %}
    )
    LOCATION '{{ table_path }}'
    TBLPROPERTIES (
        'table_type'='ICEBERG',
        'format'='parquet'
    )
    """
)

insert_sql_template = Template(
    """
    INSERT INTO {{ iceberg_database_name }}.{{ table_name }}
    SELECT * FROM {{ database_name }}.{{ table_name }}
    """
)

for table in tables:
    create_template_data = {
        "database_name": iceberg_database_name,
        "table_name": table["Name"],
        "columns": table["StorageDescriptor"]["Columns"],
        "table_path": table["StorageDescriptor"]["Location"].replace(
            "/tpcds/", "/tpcds_iceberg/"
        ),
    }

    create_sql = create_sql_template.render(**create_template_data)
    create_id = wr.athena.start_query_execution(
        create_sql, database=iceberg_database_name
    )
    create_resp = wr.athena.wait_query(query_execution_id=create_id)

    insert_sql = insert_sql_template.render(
        database_name=database_name,
        iceberg_database_name=iceberg_database_name,
        table_name=table["Name"],
    )
    insert_id = wr.athena.start_query_execution(insert_sql)
    insert_resp = wr.athena.wait_query(insert_id)
