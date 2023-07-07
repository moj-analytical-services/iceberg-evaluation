# %%
# For running in a glue interactive session
%load_ext dotenv
%dotenv
%iam_role arn:aws:iam::684969100054:role/AdminAccessGlueNotebook
%region eu-west-1
%session_id_prefix test-data-
%glue_version 3.0
%idle_timeout 60
%worker_type G.1X
%number_of_workers 2
# %%
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext

from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as f
from pyspark.sql.types import *
import boto3
import pandas as pd
from datetime import datetime

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

# %%
database_name = "tpcds_test"
bucket_name = "sb-test-bucket-ireland"
input_tables = ["full_load", "cdc_1", "cdc_2", "cdc_3"]
expected_data = ["bulk_insert", "update_1", "update_2", "update_3"]

try:
    glue = boto3.client("glue")
    glue.create_database(DatabaseInput={"Name": database_name})
    print(f"New database {database_name} created")
except glue.exceptions.AlreadyExistsException:
    print(f"Database {database_name} already exist")

## Delete files in S3
s3 = boto3.resource("s3")
bucket = s3.Bucket(bucket_name)
bucket.objects.filter(Prefix=f"{database_name}/").delete()

## Drop table in Glue Data Catalog
for t in input_tables+expected_data:
    try:
        glue = boto3.client("glue")
        glue.delete_table(DatabaseName=database_name, Name=t)
        print(f"Table {database_name}.{t} deleted")
    except glue.exceptions.EntityNotFoundException:
        print(f"Table {database_name}.{t} does not exist")

# %%
directory = f"s3://{bucket_name}/{database_name}"  # "data"
future_end_datetime = datetime(2250, 1, 1)

schema = StructType(
    [
        StructField("ss_sold_date_sk", IntegerType(), True),
        StructField("ss_sold_time_sk", IntegerType(), True),
        StructField("ss_item_sk", IntegerType(), True),
        StructField("ss_customer_sk", IntegerType(), True),
        StructField("ss_cdemo_sk", IntegerType(), True),
        StructField("ss_hdemo_sk", IntegerType(), True),
        StructField("ss_store_sk", IntegerType(), True),
        StructField("ss_addr_sk", IntegerType(), True),
        StructField("ss_promo_sk", IntegerType(), True),
        StructField("ss_ticket_number", LongType(), True),
        StructField("ss_quantity", IntegerType(), True),
        StructField("ss_wholesale_cost", DoubleType(), True),
        StructField("ss_list_price", DoubleType(), True),
        StructField("ss_sales_price", DoubleType(), True),
        StructField("ss_ext_discount_amt", DoubleType(), True),
        StructField("ss_ext_sales_price", DoubleType(), True),
        StructField("ss_ext_wholesale_cost", DoubleType(), True),
        StructField("ss_ext_list_price", DoubleType(), True),
        StructField("ss_ext_tax", DoubleType(), True),
        StructField("ss_coupon_amt", DoubleType(), True),
        StructField("ss_net_paid", DoubleType(), True),
        StructField("ss_net_paid_inc_tax", DoubleType(), True),
        StructField("ss_net_profit", DoubleType(), True),
        StructField("extraction_timestamp", TimestampType(), True),
        StructField("op", StringType(), True),
        StructField("pk", StringType(), True),
    ]
)

schema_output = StructType(
    [
        StructField("ss_sold_date_sk", IntegerType(), True),
        StructField("ss_sold_time_sk", IntegerType(), True),
        StructField("ss_item_sk", IntegerType(), True),
        StructField("ss_customer_sk", IntegerType(), True),
        StructField("ss_cdemo_sk", IntegerType(), True),
        StructField("ss_hdemo_sk", IntegerType(), True),
        StructField("ss_store_sk", IntegerType(), True),
        StructField("ss_addr_sk", IntegerType(), True),
        StructField("ss_promo_sk", IntegerType(), True),
        StructField("ss_ticket_number", LongType(), True),
        StructField("ss_quantity", IntegerType(), True),
        StructField("ss_wholesale_cost", DoubleType(), True),
        StructField("ss_list_price", DoubleType(), True),
        StructField("ss_sales_price", DoubleType(), True),
        StructField("ss_ext_discount_amt", DoubleType(), True),
        StructField("ss_ext_sales_price", DoubleType(), True),
        StructField("ss_ext_wholesale_cost", DoubleType(), True),
        StructField("ss_ext_list_price", DoubleType(), True),
        StructField("ss_ext_tax", DoubleType(), True),
        StructField("ss_coupon_amt", DoubleType(), True),
        StructField("ss_net_paid", DoubleType(), True),
        StructField("ss_net_paid_inc_tax", DoubleType(), True),
        StructField("ss_net_profit", DoubleType(), True),
        StructField("extraction_timestamp", TimestampType(), True),
        StructField("op", StringType(), True),
        StructField("pk", StringType(), True),
        StructField("start_datetime", TimestampType(), True),
        StructField("end_datetime", TimestampType(), True),
        StructField("is_current", BooleanType(), True),
    ]
)

# %%
full_load_data = [
    {
        "pk": "A",
        "ss_quantity": 1,
        "extraction_timestamp": datetime(2022, 1, 1),
    },
    {
        "pk": "B",
        "ss_quantity": 1,
        "extraction_timestamp": datetime(2022, 1, 1),
    },
]

cdc_data_1 = [
    {
        "pk": "A",
        "ss_quantity": 3,
        "extraction_timestamp": datetime(2022, 3, 1),
        "op": "U",
    },
]

cdc_data_2 = [
    {
        "pk": "C",
        "ss_quantity": 4,
        "extraction_timestamp": datetime(2022, 4, 1),
        "op": "I",
    },
]

cdc_data_3 = [
    {
        "pk": "A",
        "ss_quantity": 2,
        "extraction_timestamp": datetime(2022, 2, 1),
        "op": "U",
    },
]
# %%
for data, t in zip([full_load_data, cdc_data_1, cdc_data_2, cdc_data_3], input_tables):
    df = spark.createDataFrame(data, schema=schema)
    dyf = DynamicFrame.fromDF(df, glue_context, "dyf")
    sink = glue_context.getSink(
        connection_type="s3",
        path=f"{directory}/{t}",
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
    )
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase=database_name, catalogTableName=t)
    sink.writeFrame(dyf)


# %%
bulk_insert_data = [
    {
        "pk": "A",
        "ss_quantity": 1,
        "extraction_timestamp": datetime(2022, 1, 1),
        "start_datetime": datetime(2022, 1, 1),
        "end_datetime": future_end_datetime,
        "is_current": True,
    },
    {
        "pk": "B",
        "ss_quantity": 1,
        "extraction_timestamp": datetime(2022, 1, 1),
        "start_datetime": datetime(2022, 1, 1),
        "end_datetime": future_end_datetime,
        "is_current": True,
    },
]
update_data_1 = [
    {
        "pk": "A",
        "ss_quantity": 1,
        "extraction_timestamp": datetime(2022, 1, 1),
        "start_datetime": datetime(2022, 1, 1),
        "end_datetime": datetime(2022, 3, 1),
        "is_current": False,
    },
    {
        "pk": "A",
        "ss_quantity": 3,
        "extraction_timestamp": datetime(2022, 3, 1),
        "op": "U",
        "start_datetime": datetime(2022, 3, 1),
        "end_datetime": future_end_datetime,
        "is_current": True,
    },
    {
        "pk": "B",
        "ss_quantity": 1,
        "extraction_timestamp": datetime(2022, 1, 1),
        "start_datetime": datetime(2022, 1, 1),
        "end_datetime": future_end_datetime,
        "is_current": True,
    },
]
update_data_2 = [
    {
        "pk": "A",
        "ss_quantity": 1,
        "extraction_timestamp": datetime(2022, 1, 1),
        "start_datetime": datetime(2022, 1, 1),
        "end_datetime": datetime(2022, 3, 1),
        "is_current": False,
    },
    {
        "pk": "A",
        "ss_quantity": 3,
        "extraction_timestamp": datetime(2022, 3, 1),
        "op": "U",
        "start_datetime": datetime(2022, 3, 1),
        "end_datetime": future_end_datetime,
        "is_current": True,
    },
    {
        "pk": "B",
        "ss_quantity": 1,
        "extraction_timestamp": datetime(2022, 1, 1),
        "start_datetime": datetime(2022, 1, 1),
        "end_datetime": future_end_datetime,
        "is_current": True,
    },
    {
        "pk": "C",
        "ss_quantity": 4,
        "extraction_timestamp": datetime(2022, 4, 1),
        "op": "I",
        "start_datetime": datetime(2022, 4, 1),
        "end_datetime": future_end_datetime,
        "is_current": True,
    },
]
update_data_3 = [
    {
        "pk": "A",
        "ss_quantity": 1,
        "extraction_timestamp": datetime(2022, 1, 1),
        "start_datetime": datetime(2022, 1, 1),
        "end_datetime": datetime(2022, 2, 1),
        "is_current": False,
    },
    {
        "pk": "A",
        "ss_quantity": 2,
        "extraction_timestamp": datetime(2022, 2, 1),
        "op": "U",
        "start_datetime": datetime(2022, 2, 1),
        "end_datetime": datetime(2022, 3, 1),
        "is_current": False,
    },
    {
        "pk": "A",
        "ss_quantity": 3,
        "extraction_timestamp": datetime(2022, 3, 1),
        "op": "U",
        "start_datetime": datetime(2022, 3, 1),
        "end_datetime": future_end_datetime,
        "is_current": True,
    },
    {
        "pk": "B",
        "ss_quantity": 1,
        "extraction_timestamp": datetime(2022, 1, 1),
        "start_datetime": datetime(2022, 1, 1),
        "end_datetime": future_end_datetime,
        "is_current": True,
    },
    {
        "pk": "C",
        "ss_quantity": 4,
        "extraction_timestamp": datetime(2022, 4, 1),
        "op": "I",
        "start_datetime": datetime(2022, 4, 1),
        "end_datetime": future_end_datetime,
        "is_current": True,
    },
]
for data, t in zip(
    [bulk_insert_data, update_data_1, update_data_2, update_data_3], expected_data
):
    # df = spark.createDataFrame(data, schema=schema_output)
    # df.repartition(1).write.parquet(f"{directory}/{t}")
    df = spark.createDataFrame(data, schema=schema_output)
    dyf = DynamicFrame.fromDF(df, glue_context, "dyf")
    sink = glue_context.getSink(
        connection_type="s3",
        path=f"{directory}/{t}",
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
    )
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase=database_name, catalogTableName=t)
    sink.writeFrame(dyf)
