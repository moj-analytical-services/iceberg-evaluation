# %%
# For running in a glue interactive session
# %load_ext dotenv
# %dotenv
# %iam_role arn:aws:iam::684969100054:role/AdminAccessGlueNotebook
# %region eu-west-1
# %session_id_prefix pandas-
# %glue_version 3.0
# %idle_timeout 60
# %worker_type G.1X
# %number_of_workers 2
# %%
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext

import pyspark.sql.functions as f
from pyspark.sql.types import *
import boto3
import pandas as pd
from datetime import datetime

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

# %%
directory = "data/" # "s3://sb-test-bucket-ireland/test-data/" 
full_load_path = f"{directory}store_sales"
cdc_path_1 = f"{directory}cdc_1"
cdc_path_2 = f"{directory}cdc_2"
cdc_path_3 = f"{directory}cdc_3"
bulk_insert_path = f"{directory}bulk_insert"
update_path_1 = f"{directory}update_1"
update_path_2 = f"{directory}update_2"
update_path_3 = f"{directory}update_3"


full_load_extraction_timestamp = datetime(2022, 1, 1)
cdc_extraction_timestamp_1 = datetime(2022, 2, 1)
cdc_extraction_timestamp_2 = datetime(2022, 4, 1)
cdc_extraction_timestamp_3 = datetime(2022, 3, 1)
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
        "extraction_timestamp": full_load_extraction_timestamp,
    },
    {
        "pk": "B",
        "ss_quantity": 1,
        "extraction_timestamp": full_load_extraction_timestamp,
    },
]
full_load_df = spark.createDataFrame(full_load_data, schema=schema)
full_load_df.repartition(1).write.parquet(full_load_path)
pd.read_parquet(full_load_path)

# %%
# update
cdc_data_1 = [
    {
        "pk": "A",
        "ss_quantity": 3,
        "extraction_timestamp": cdc_extraction_timestamp_1,
        "op": "U",
    },
]
cdc_df_1 = spark.createDataFrame(cdc_data_1, schema=schema)
cdc_df_1.repartition(1).write.parquet(cdc_path_1)
pd.read_parquet(cdc_path_1)

# %%
# insert
cdc_data_2 = [
    {
        "pk": "C",
        "ss_quantity": 1,
        "extraction_timestamp": cdc_extraction_timestamp_2,
        "op": "I",
    },
]
cdc_df_2 = spark.createDataFrame(cdc_data_2, schema=schema)
cdc_df_2.repartition(1).write.parquet(cdc_path_2)
pd.read_parquet(cdc_path_2)

# %%
# late arriving
cdc_data_3 = [
    {
        "pk": "A",
        "ss_quantity": 2,
        "extraction_timestamp": cdc_extraction_timestamp_3,
        "op": "U",
    },
]
cdc_df_3 = spark.createDataFrame(cdc_data_3, schema=schema)
cdc_df_3.repartition(1).write.parquet(cdc_path_3)
pd.read_parquet(cdc_path_3)

# %%
bulk_insert_data = [
    {
        "pk": "A",
        "ss_quantity": 1,
        "extraction_timestamp": full_load_extraction_timestamp,
        "start_datetime": full_load_extraction_timestamp,
        "end_datetime": future_end_datetime,
        "is_current": True,
    },
    {
        "pk": "B",
        "ss_quantity": 1,
        "extraction_timestamp": full_load_extraction_timestamp,
        "start_datetime": full_load_extraction_timestamp,
        "end_datetime": future_end_datetime,
        "is_current": True,
    },
]
bulk_insert_df = spark.createDataFrame(bulk_insert_data, schema=schema_output)
bulk_insert_df.repartition(1).write.parquet(bulk_insert_path)
pd.read_parquet(bulk_insert_path)

# %%
update_data_1 = [
    {
        "pk": "A",
        "ss_quantity": 1,
        "extraction_timestamp": full_load_extraction_timestamp,
        "start_datetime": cdc_extraction_timestamp_1,
        "end_datetime": future_end_datetime,
        "is_current": False,
    },
    {
        "pk": "A",
        "ss_quantity": 3,
        "extraction_timestamp": cdc_extraction_timestamp_1,
        "op": "U",
        "start_datetime": cdc_extraction_timestamp_1,
        "end_datetime": future_end_datetime,
        "is_current": True,
    },
    {
        "pk": "B",
        "ss_quantity": 1,
        "extraction_timestamp": full_load_extraction_timestamp,
        "start_datetime": full_load_extraction_timestamp,
        "end_datetime": future_end_datetime,
        "is_current": True,
    },
]
update_df_1 = spark.createDataFrame(update_data_1, schema=schema_output)
update_df_1.repartition(1).write.parquet(update_path_1)
pd.read_parquet(update_path_1)

# %%
update_data_2 = [
    {
        "pk": "A",
        "ss_quantity": 1,
        "extraction_timestamp": full_load_extraction_timestamp,
        "start_datetime": cdc_extraction_timestamp_1,
        "end_datetime": future_end_datetime,
        "is_current": False,
    },
    {
        "pk": "A",
        "ss_quantity": 3,
        "extraction_timestamp": cdc_extraction_timestamp_1,
        "op": "U",
        "start_datetime": cdc_extraction_timestamp_1,
        "end_datetime": future_end_datetime,
        "is_current": True,
    },
    {
        "pk": "B",
        "ss_quantity": 1,
        "extraction_timestamp": full_load_extraction_timestamp,
        "start_datetime": full_load_extraction_timestamp,
        "end_datetime": future_end_datetime,
        "is_current": True,
    },
    {
        "pk": "C",
        "ss_quantity": 1,
        "extraction_timestamp": cdc_extraction_timestamp_2,
        "op": "I",
        "start_datetime": cdc_extraction_timestamp_2,
        "end_datetime": future_end_datetime,
        "is_current": True,
    },
]
update_df_2 = spark.createDataFrame(update_data_2, schema=schema_output)
update_df_2.repartition(1).write.parquet(update_path_2)
pd.read_parquet(update_path_2)

# %%
update_data_3 = [
    {
        "pk": "A",
        "ss_quantity": 1,
        "extraction_timestamp": full_load_extraction_timestamp,
        "start_datetime": cdc_extraction_timestamp_3,
        "end_datetime": future_end_datetime,
        "is_current": False,
    },
    {
        "pk": "A",
        "ss_quantity": 2,
        "extraction_timestamp": cdc_extraction_timestamp_3,
        "op": "U",
        "start_datetime": cdc_extraction_timestamp_3,
        "end_datetime": cdc_extraction_timestamp_2,
        "is_current": False,
    },
    {
        "pk": "A",
        "ss_quantity": 3,
        "extraction_timestamp": cdc_extraction_timestamp_2,
        "op": "U",
        "start_datetime": cdc_extraction_timestamp_2,
        "end_datetime": future_end_datetime,
        "is_current": True,
    },
    {
        "pk": "B",
        "ss_quantity": 1,
        "extraction_timestamp": full_load_extraction_timestamp,
        "start_datetime": full_load_extraction_timestamp,
        "end_datetime": future_end_datetime,
        "is_current": True,
    },
    {
        "pk": "C",
        "ss_quantity": 1,
        "extraction_timestamp": cdc_extraction_timestamp_2,
        "op": "I",
        "start_datetime": cdc_extraction_timestamp_2,
        "end_datetime": future_end_datetime,
        "is_current": True,
    },
]
update_df_3 = spark.createDataFrame(update_data_3, schema=schema_output)
update_df_3.repartition(1).write.parquet(update_path_3)
pd.read_parquet(update_path_3)


