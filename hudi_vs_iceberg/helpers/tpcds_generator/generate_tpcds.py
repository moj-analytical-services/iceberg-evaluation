import boto3
import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import pyspark.sql.functions as f
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkConf

bucket_name = "sb-test-bucket-ireland"
scale = 1 #3000
partitions = 36 #396
extraction_timestamp = "2022-01-01"
table_format = "_hudi" # options: "_iceberg"  "_hudi". "" for hive
database_name = f"tpcds{table_format}_{scale}"

if table_format == "":
    glue_context = GlueContext(SparkContext())
    spark = glue_context.spark_session

if table_format == "_hudi":
    conf = SparkConf()
    conf.set("spark.default.parallelism", partitions)
    conf.set("spark.sql.shuffle.partitions", partitions)
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.sql.hive.convertMetastoreParquet","false")
    glue_context = GlueContext(SparkContext.getOrCreate(conf=conf))
    spark = glue_context.spark_session

tables = {
    # "call_center", "catalog_returns",
    # "catalog_page", "catalog_sales",
    # "customer", "customer_address", "customer_demographics",
    # "date_dim", "dbgen_version", "household_demographics",
    # "income_band", "inventory", "item", "promotion", "reason",
    # "ship_mode", "store", "store_returns",
    # "store_sales",
    "time_dim":"t_time_sk",
    "warehouse":"w_warehouse_id",
    # "web_page", "web_returns", "web_sales", "web_site"
}


## Create a database if not exists
try:
    glue = boto3.client("glue")
    glue.create_database(DatabaseInput={"Name": database_name})
    print(f"New database {database_name} created")
except glue.exceptions.AlreadyExistsException:
    print(f"Database {database_name} already exist")


for table,primary_key in tables.items():
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
        }
    )
    
    # Add extra columns
    df = df.withColumn(
            "extraction_timestamp",
            f.to_timestamp(f.lit(extraction_timestamp), "yyyy-MM-dd"),
        )
    df = df.withColumn("op", f.lit(None).cast("string"))
    if table == "store_sales":
        df = df.withColumn(
                "pk",
                f.concat(
                    f.col("ss_ticket_number").cast("string"),
                    f.lit("-"),
                    f.col("ss_item_sk").cast("string"),
                ),
            )

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
            'hoodie.table.name': table,
            'hoodie.datasource.write.storage.type': 'COPY_ON_WRITE',
            'hoodie.datasource.write.recordkey.field': primary_key,
            #'hoodie.datasource.write.partitionpath.field': 'extraction_timestamp',
            'hoodie.datasource.write.table.name': table,
            'hoodie.datasource.write.operation': 'upsert',
            'hoodie.datasource.write.precombine.field': 'extraction_timestamp',
            'hoodie.datasource.write.hive_style_partitioning': 'true',
            'hoodie.upsert.shuffle.parallelism': partitions,
            'hoodie.insert.shuffle.parallelism': partitions,
            'path': f"s3://{bucket_name}/{path}",
            'hoodie.datasource.hive_sync.enable': 'true',
            'hoodie.datasource.hive_sync.database': database_name,
            'hoodie.datasource.hive_sync.table': table,
            # 'hoodie.datasource.hive_sync.partition_fields': 'extraction_timestamp',
            'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
            'hoodie.datasource.hive_sync.use_jdbc': 'false',
            'hoodie.datasource.hive_sync.mode': 'hms'
        }
        
        df.write.format("hudi")  \
        .options(**hudi_options)  \
        .mode("overwrite")  \
        .save()
