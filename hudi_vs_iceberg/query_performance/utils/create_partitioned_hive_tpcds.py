import os
import re

import awswrangler as wr
import boto3
from mojap_metadata.converters.glue_converter import GlueConverter, GlueTable

os.environ["AWS_DEFAULT_REGION"] = "eu-west-1"

wr.config.workgroup = "AthenaWorkGroupIceberg"

existing_database_name = "tpcds_1"
database_name = "tpcds_3000_partitioned"
bucket_name = "sb-test-bucket-ireland"
database_prefix = "blog/BLOG_TPCDS-TEST-3T-partitioned/"
database_path = f"s3://{bucket_name}/{database_prefix}"

databases = wr.catalog.databases(limit=None)
if database_name not in databases.Database.to_list():
    wr.catalog.create_database(database_name)

tables = [
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
]

for table_name in tables:
    print(f"Running {table_name}")
    client = boto3.client("s3")
    table_prefix = f"{database_prefix}{table_name}/"

    print(f"Checking for partitions for {table_name}")
    result = client.list_objects(Bucket=bucket_name, Prefix=table_prefix, Delimiter="/")

    if result.get("CommonPrefixes") is not None:
        results = [
            o.get("Prefix").replace(table_prefix, "")
            for o in result.get("CommonPrefixes")
        ]
    else:
        results = []

    partitions = {re.sub("=.*", "", f) for f in results}

    gt = GlueTable()
    meta = gt.generate_to_meta(existing_database_name, table_name)
    meta.file_format = "parquet"

    partitions_not_in_meta = [p for p in partitions if p not in meta.column_names]

    if not partitions_not_in_meta:
        meta.partitions = list(partitions)
    else:
        raise ValueError(f"partition is missing from {table_name}")

    # Line for outputting metadata
    # meta.to_json(
    #   f"hudi_vs_iceberg/query_performance/utils/metadata/{table_name}.json",
    #   indent=4,
    # )

    glue_client = boto3.client("glue")

    gc = GlueConverter()

    print(f"Getting metadata from existing {table_name} athena table")
    spec = gc.generate_from_meta(
        meta,
        database_name=database_name,
        table_location=f"s3://{bucket_name}/{table_prefix}",
    )

    print(f"Creating table in glue for {table_name}")
    _ = glue_client.create_table(**spec)

    if partitions:
        print(f"Repairing partitions for {table_name}")
        response = wr.athena.repair_table(table=table_name, database=database_name)

        if response != "SUCCEEDED":
            raise ValueError("Repair failed")
    else:
        print("No partitions to repair")
