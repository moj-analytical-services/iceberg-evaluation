import boto3
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import pyspark.sql.functions as f
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkConf


def _getOptionalResolvedOptions(arguments):
    args_dict = {}
    for i in range(1, len(arguments), 2):
        arg_name = arguments[i].lstrip("--")
        arg_value = arguments[i + 1]
        args_dict[arg_name] = arg_value
    return args_dict


args = _getOptionalResolvedOptions(sys.argv)

bucket_name = args.get("bucket", "sb-test-bucket-ireland")
table_filter = args.get("table", "store_sales")
scale = args.get("scale", 1)
table_format = args.get("table_format", "")  # options: "_iceberg"  "_hudi". "" for hive
partitions = 36  # 396
extraction_timestamp = "2022-01-01"
database_name = f"tpcds{table_format}_{scale}"

if table_format == "":
    glue_context = GlueContext(SparkContext())
    spark = glue_context.spark_session

if table_format == "_hudi":
    conf = SparkConf()
    conf.set("spark.default.parallelism", partitions)
    conf.set("spark.sql.shuffle.partitions", partitions)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.sql.hive.convertMetastoreParquet", "false")
    glue_context = GlueContext(SparkContext.getOrCreate(conf=conf))
    spark = glue_context.spark_session

tables = {
    "call_center": "cc_call_center_sk",
    "catalog_returns": ["cr_item_sk", "cr_order_number"],
    "catalog_page": "cp_catalog_page_sk",
    "catalog_sales": ["cs_item_sk", "cs_order_number"],
    "customer": "c_customer_sk",
    "customer_address": "ca_address_sk",
    "customer_demographics": "cd_demo_sk",
    "date_dim": "d_date_sk",
    "dbgen_version": "dv_version",
    "household_demographics": "hd_demo_sk",
    "income_band": "ib_income_band_sk",
    "inventory": ["inv_date_sk", "inv_item_sk", "inv_warehouse_sk"],
    "item": "i_item_sk",
    "promotion": "p_promo_sk",
    "reason": "r_reason_sk",
    "ship_mode": "sm_ship_mode_sk",
    "store": "s_store_sk",
    "store_returns": ["sr_item_sk", "sr_ticket_number"],
    "store_sales": ["ss_item_sk", "ss_ticket_number"],
    "time_dim": "t_time_sk",
    "warehouse": "w_warehouse_id",
    "web_page": "wp_web_page_sk",
    "web_returns": ["wr_item_sk", "wr_order_number"],
    "web_sales": ["ws_item_sk", "ws_order_number"],
    "web_site": "web_site_sk",
}
if table_filter:
    tables = {table_filter: tables[table_filter]}

## Create a database if not exists
try:
    glue = boto3.client("glue")
    glue.create_database(DatabaseInput={"Name": database_name})
    print(f"New database {database_name} created")
except glue.exceptions.AlreadyExistsException:
    print(f"Database {database_name} already exist")


for table, primary_key in tables.items():
    path = f"tpcds{table_format}/scale={scale}/table={table}/"

    ## Delete files in S3
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    bucket.objects.filter(Prefix=path).delete()

    ## Drop table in Glue Data Catalog
    try:
        glue = boto3.client("glue")
        glue.delete_table(DatabaseName=database_name, Name=table)
    except glue.exceptions.EntityNotFoundException:
        print(f"Table {database_name}.{table} does not exist")

    # Create a dataframe for `table`
    df = glue_context.create_data_frame.from_options(
        connection_type="marketplace.spark",
        connection_options={
            "table": table,
            "scale": scale,
            "numPartitions": partitions,
            "connectionName": "tpcds-connector-glue3",
        },
    )

    # Add extra columns
    df = df.withColumn(
        "extraction_timestamp",
        f.to_timestamp(f.lit(extraction_timestamp), "yyyy-MM-dd"),
    )
    df = df.withColumn("op", f.lit(None).cast("string"))

    if isinstance(primary_key, list):
        df = df.withColumn("pk", f.md5(f.concat(*primary_key)))
        primary_key = "pk"

    if table_format == "":
        dyf2 = DynamicFrame.fromDF(df, glue_context, "dyf2")
        sink = glue_context.getSink(
            connection_type="s3",
            path=f"s3://{bucket_name}/{path}",
            enableUpdateCatalog=True,
            updateBehavior="UPDATE_IN_DATABASE",
        )
        sink.setFormat("glueparquet")
        sink.setCatalogInfo(catalogDatabase=database_name, catalogTableName=table)
        sink.writeFrame(dyf2)

    if table_format == "_hudi":
        hudi_options = {
            "hoodie.table.name": table,
            "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
            "hoodie.datasource.write.recordkey.field": primary_key,
            #'hoodie.datasource.write.partitionpath.field': 'extraction_timestamp',
            "hoodie.datasource.write.table.name": table,
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.datasource.write.precombine.field": "extraction_timestamp",
            "hoodie.datasource.write.hive_style_partitioning": "true",
            "hoodie.upsert.shuffle.parallelism": partitions,
            "hoodie.insert.shuffle.parallelism": partitions,
            "path": f"s3://{bucket_name}/{path}",
            "hoodie.datasource.hive_sync.enable": "true",
            "hoodie.datasource.hive_sync.database": database_name,
            "hoodie.datasource.hive_sync.table": table,
            # 'hoodie.datasource.hive_sync.partition_fields': 'extraction_timestamp',
            "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
            "hoodie.datasource.hive_sync.use_jdbc": "false",
            "hoodie.datasource.hive_sync.mode": "hms",
        }

        df.write.format("hudi").options(**hudi_options).mode("overwrite").save()
