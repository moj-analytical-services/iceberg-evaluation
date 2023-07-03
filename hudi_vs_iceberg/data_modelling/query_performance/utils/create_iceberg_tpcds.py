import os
from itertools import chain

import awswrangler as wr
from jinja2 import Template

os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"

wr.config.workgroup = "AthenaWorkGroupIceberg"

database_name = "tpcds_3000_partitioned"
iceberg_database_name = "tpcds_iceberg_3000_partitioned"
current_prefix = "/BLOG_TPCDS-TEST-3T-partitioned/"
new_prefix = "/BLOG_TPCDS-TEST-3T-partitioned-iceberg/"

tables = wr.catalog.get_tables(database=database_name)

wr.catalog.create_database(name=iceberg_database_name)

create_sql_template = Template(
    """
    CREATE TABLE {{ database_name }}.{{ table_name }}(
    {% for column in columns %}
        {{ column["Name"] }} {{ column["Type"] }}{{ ", " if not loop.last else "" }}
    {% endfor %}
    )
    {% if partitions %}
        PARTITIONED BY (
            {% for partition in partitions %}
                {{ partition }}{{ ", " if not loop.last else "" }}
            {% endfor %}
        )
    {% endif %}
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
    {% if partitions %}
        WHERE {{ partition_key }} IN (
            {% for partition in partitions %}
                {{ partition }}{{ ", " if not loop.last else "" }}
            {% endfor %}
        )
    {% endif %}
    """
)

for table in tables:
    print(f"Creating iceberg table for {table}")

    table_partitions = [p["Name"] for p in table["PartitionKeys"]]
    create_template_data = {
        "database_name": iceberg_database_name,
        "table_name": table["Name"],
        "columns": table["StorageDescriptor"]["Columns"] + table["PartitionKeys"],
        "partitions": table_partitions,
        "table_path": table["StorageDescriptor"]["Location"].replace(
            current_prefix, new_prefix
        ),
    }

    create_sql = create_sql_template.render(**create_template_data)
    create_id = wr.athena.start_query_execution(
        create_sql, database=iceberg_database_name
    )
    create_resp = wr.athena.wait_query(query_execution_id=create_id)

    if len(table_partitions) == 1:
        loaded_partitions = wr.catalog.get_partitions(database_name, table["Name"])
        updated_partitions = list(chain(*[p for _, p in loaded_partitions.items()]))
        updated_partitions = [
            p if p.lower() != "__hive_default_partition__" else "NULL"
            for p in updated_partitions
        ]
        step = 100
        for i in range(100, len(updated_partitions), step):
            print(f"Inserting partitions {i} to {i + step}")
            current_partitions = updated_partitions[i : i + step]
            insert_sql = insert_sql_template.render(
                database_name=database_name,
                iceberg_database_name=iceberg_database_name,
                table_name=table["Name"],
                partition_key=table_partitions[0],
                partitions=current_partitions,
            )
            insert_id = wr.athena.start_query_execution(insert_sql)
            insert_resp = wr.athena.wait_query(insert_id)

    else:
        insert_sql = insert_sql_template.render(
            database_name=database_name,
            iceberg_database_name=iceberg_database_name,
            table_name=table["Name"],
        )
        insert_id = wr.athena.start_query_execution(insert_sql)
        insert_resp = wr.athena.wait_query(insert_id)
