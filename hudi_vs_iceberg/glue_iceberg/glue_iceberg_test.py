# %%
%load_ext dotenv
%dotenv
%iam_role arn:aws:iam::684969100054:role/AdminAccessGlueNotebook
%region eu-west-1
%session_id_prefix test-
%glue_version 3.0
%idle_timeout 60
%worker_type G.1X
%number_of_workers 2
%%configure
{
  "--datalake-formats": "iceberg",
  "--conf": "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
}

# %%
# Set parameters and create session

import boto3
import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import SparkSession

catalog_name = "glue_catalog"
bucket_name = "sb-test-bucket-ireland"
bucket_prefix = "sb"
table_name = "datagensb"
warehouse_path = f"s3://{bucket_name}/{bucket_prefix}"
input_prefix = "tpcds_test"
input_path = f"s3://{bucket_name}/{input_prefix}"
source_database_name = "tpcds_test"
dest_database_name = "tpcds_test_glue_iceberg"
output_directory = f"{catalog_name}.{dest_database_name}.{table_name}"
future_end_datetime = datetime.datetime(2250, 1, 1)

spark = (
    SparkSession.builder.config(
        f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"{warehouse_path}")
    .config(
        f"spark.sql.catalog.{catalog_name}.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    .config(
        f"spark.sql.catalog.{catalog_name}.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .getOrCreate()
)

# %%
# Prep for databases and files

## Create a database with the name hudi_df to host hudi tables if not exists.
try:
    glue = boto3.client("glue")
    glue.create_database(DatabaseInput={"Name": dest_database_name})
    print(f"New database {dest_database_name} created")
except glue.exceptions.AlreadyExistsException:
    print(f"Database {dest_database_name} already exist")

## Delete files in S3
s3 = boto3.resource("s3")
bucket = s3.Bucket(bucket_name)
bucket.objects.filter(Prefix=f"{bucket_name}/{bucket_prefix}/").delete()

# %%
# Show Full Load data

spark.read.option("header", "true").parquet(f"{input_path}/full_load").show()

# %%
# Run Bulk Insert

## Drop table in Glue Data Catalog if it exists
try:
    glue = boto3.client("glue")
    glue.delete_table(DatabaseName=dest_database_name, Name=table_name)
    print(f"Table {dest_database_name}.{table_name} deleted")
except glue.exceptions.EntityNotFoundException:
    print(f"Table {dest_database_name}.{table_name} does not exist")


def bulk_insert(full_load_path, output_directory, future_end_datetime):
    # read the bulk insert parquet file
    full_load = spark.read.parquet(full_load_path)
    # adds 3 new columns
    full_load = full_load.withColumn("start_datetime", F.col("extraction_timestamp"))
    full_load = full_load.withColumn(
        "end_datetime", F.to_timestamp(F.lit(future_end_datetime), "yyyy-MM-dd")
    )
    full_load = full_load.withColumn("op", F.lit("None"))
    full_load = full_load.withColumn("is_current", F.lit(True))
    full_load.writeTo(output_directory).createOrReplace()


bulk_insert(f"{input_path}/full_load", output_directory, future_end_datetime)
spark.table(output_directory).show()

# %%
# Show first cdc data (update)

spark.read.option("header", "true").parquet(f"{input_path}/cdc_1").show()

# %%
# Run scd2_simple on first cdc data


def scd2_simple(updates_filepath, output_directory, future_end_datetime, primary_key):
    # read the new updates parquet file
    updates = spark.read.option("header", "true").parquet(updates_filepath)
    # adds 3 new columns
    updates = updates.withColumn("start_datetime", F.col("extraction_timestamp"))
    updates = updates.withColumn(
        "end_datetime", F.to_timestamp(F.lit(future_end_datetime), "yyyy-MM-dd")
    )
    updates = updates.withColumn("is_current", F.lit(True))
    updates.createOrReplaceTempView(f"tmp_{table_name}_updates")
    simple_merge_sql = f"""
    MERGE INTO {output_directory} dest
        USING (   
            SELECT
            ss_sold_date_sk,
            ss_sold_time_sk,
            ss_item_sk,
            ss_customer_sk,
            ss_cdemo_sk,
            ss_hdemo_sk,
            ss_addr_sk, 
            ss_store_sk,
            ss_promo_sk,
            ss_ticket_number,
            ss_quantity,
            ss_wholesale_cost,
            ss_list_price,
            ss_sales_price, 
            ss_ext_discount_amt,
            ss_ext_sales_price,
            ss_ext_wholesale_cost,
            ss_ext_list_price,
            ss_ext_tax,
            ss_coupon_amt,
            ss_net_paid, 
            ss_net_paid_inc_tax,
            ss_net_profit, 
            extraction_timestamp, 
            op, 
            pk,
            start_datetime, 
            end_datetime, 
            is_current
                FROM tmp_{table_name}_updates
        UNION ALL
            SELECT
            t.ss_sold_date_sk,
            t.ss_sold_time_sk, 
            t.ss_item_sk,
            t.ss_customer_sk,
            t.ss_cdemo_sk,
            t.ss_hdemo_sk,
            t.ss_addr_sk, 
            t.ss_store_sk,
            t.ss_promo_sk,
            t.ss_ticket_number,
            t.ss_quantity, 
            t.ss_wholesale_cost,
            t.ss_list_price,
            t.ss_sales_price, 
            t.ss_ext_discount_amt,
            t.ss_ext_sales_price,
            t.ss_ext_wholesale_cost, 
            t.ss_ext_list_price,
            t.ss_ext_tax,
            t.ss_coupon_amt,
            t.ss_net_paid, 
            t.ss_net_paid_inc_tax,
            t.ss_net_profit, 
            t.extraction_timestamp, 
            t.op, 
            t.pk,
            t.start_datetime,
            u.start_datetime AS end_datetime, 
            u.is_current
            FROM {output_directory} as t
            INNER JOIN tmp_{table_name}_updates as u ON t.pk = u.pk AND t.is_current = true
        ) AS src
        ON (dest.pk = src.pk AND dest.extraction_timestamp=src.extraction_timestamp)
        WHEN MATCHED THEN
            UPDATE SET end_datetime = src.end_datetime,is_current = false
        WHEN NOT MATCHED THEN 
            INSERT (
                ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, 
                ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, 
                ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, 
                ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, extraction_timestamp, op, pk, start_datetime, end_datetime, is_current
            )
            VALUES (
                src.ss_sold_date_sk, src.ss_sold_time_sk, src.ss_item_sk, src.ss_customer_sk, src.ss_cdemo_sk, src.ss_hdemo_sk, 
                src.ss_addr_sk, src.ss_store_sk, src.ss_promo_sk, src.ss_ticket_number, src.ss_quantity, src.ss_wholesale_cost,
                src.ss_list_price, src.ss_sales_price, src.ss_ext_discount_amt, src.ss_ext_sales_price, src.ss_ext_wholesale_cost, 
                src.ss_ext_list_price, src.ss_ext_tax, src.ss_coupon_amt, src.ss_net_paid, src.ss_net_paid_inc_tax, src.ss_net_profit,
                src.extraction_timestamp, src.op, src.pk, src.extraction_timestamp, src.end_datetime, true
            )
            """
    spark.sql(simple_merge_sql)


scd2_simple(f"{input_path}/cdc_1", output_directory, future_end_datetime, "pk")
spark.table(output_directory).sort("pk", "extraction_timestamp").show()

# %%
# Show second cdc data (insert)

spark.read.option("header", "true").parquet(f"{input_path}/cdc_2").show()

# %%
# Run scd2_simple on second cdc data

scd2_simple(f"{input_path}/cdc_2", output_directory, future_end_datetime, "pk")
spark.table(output_directory).sort("pk", "extraction_timestamp").show()


# %%
def scd2_complex(updates_filepath, output_directory, future_end_datetime, primary_key):
    # read the new updates parquet file
    updates = spark.read.option("header", "true").parquet(updates_filepath)
    # adds 3 new columns
    updates = updates.withColumn("start_datetime", F.col("extraction_timestamp"))
    updates = updates.withColumn(
        "end_datetime", F.to_timestamp(F.lit(future_end_datetime), "yyyy-MM-dd")
    )
    updates = updates.withColumn("is_current", F.lit(True))
    updates.createOrReplaceTempView(f"tmp_{table_name}_updates")
    
    scd2_rows = f"""
    WITH t1 AS (
        SELECT
            ss_sold_date_sk,
            ss_sold_time_sk,
            ss_item_sk,
            ss_customer_sk,
            ss_cdemo_sk,
            ss_hdemo_sk,
            ss_addr_sk, 
            ss_store_sk,
            ss_promo_sk,
            ss_ticket_number,
            ss_quantity,
            ss_wholesale_cost,
            ss_list_price,
            ss_sales_price, 
            ss_ext_discount_amt,
            ss_ext_sales_price,
            ss_ext_wholesale_cost,
            ss_ext_list_price,
            ss_ext_tax,
            ss_coupon_amt,
            ss_net_paid, 
            ss_net_paid_inc_tax,
            ss_net_profit, 
            extraction_timestamp, 
            op, 
            pk,
            start_datetime
                FROM tmp_{table_name}_updates
        UNION ALL
            SELECT
            t.ss_sold_date_sk,
            t.ss_sold_time_sk, 
            t.ss_item_sk,
            t.ss_customer_sk,
            t.ss_cdemo_sk,
            t.ss_hdemo_sk,
            t.ss_addr_sk, 
            t.ss_store_sk,
            t.ss_promo_sk,
            t.ss_ticket_number,
            t.ss_quantity, 
            t.ss_wholesale_cost,
            t.ss_list_price,
            t.ss_sales_price, 
            t.ss_ext_discount_amt,
            t.ss_ext_sales_price,
            t.ss_ext_wholesale_cost, 
            t.ss_ext_list_price,
            t.ss_ext_tax,
            t.ss_coupon_amt,
            t.ss_net_paid, 
            t.ss_net_paid_inc_tax,
            t.ss_net_profit, 
            t.extraction_timestamp, 
            t.op, 
            t.pk,
            t.start_datetime
            FROM {output_directory} as t
            INNER JOIN tmp_{table_name}_updates as u ON t.pk = u.pk
            ),
        t2 AS (
            SELECT *,
                    LEAD(extraction_timestamp,1,TO_TIMESTAMP('2250-01-01 00:00:00'))
                    OVER(PARTITION BY {primary_key} ORDER BY extraction_timestamp) AS end_datetime
            FROM t1
        )
        SELECT *,
            (CASE WHEN end_datetime=TO_TIMESTAMP('2250-01-01 00:00:00') THEN true ELSE false END) AS is_current
        FROM t2
        ORDER BY {primary_key}, extraction_timestamp
    """
    
    
    complex_merge_sql = f"""
    MERGE INTO {output_directory} dest
        USING ({scd2_rows}) as src
        ON (dest.pk = src.pk AND dest.extraction_timestamp=src.extraction_timestamp)
        WHEN MATCHED THEN
            UPDATE SET end_datetime = src.end_datetime,is_current = src.is_current
        WHEN NOT MATCHED THEN 
            INSERT (
                ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, 
                ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, 
                ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, 
                ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, extraction_timestamp, op, pk, start_datetime, end_datetime, is_current
            )
            VALUES (
                src.ss_sold_date_sk, src.ss_sold_time_sk, src.ss_item_sk, src.ss_customer_sk, src.ss_cdemo_sk, src.ss_hdemo_sk, 
                src.ss_addr_sk, src.ss_store_sk, src.ss_promo_sk, src.ss_ticket_number, src.ss_quantity, src.ss_wholesale_cost,
                src.ss_list_price, src.ss_sales_price, src.ss_ext_discount_amt, src.ss_ext_sales_price, src.ss_ext_wholesale_cost, 
                src.ss_ext_list_price, src.ss_ext_tax, src.ss_coupon_amt, src.ss_net_paid, src.ss_net_paid_inc_tax, src.ss_net_profit,
                src.extraction_timestamp, src.op, src.pk, src.extraction_timestamp, src.end_datetime, src.is_current
            )
            """
    spark.sql(complex_merge_sql)

scd2_complex(f"{input_path}/cdc_3", output_directory, future_end_datetime, "pk")
spark.table(output_directory).sort("pk", "extraction_timestamp").show()
# %%
