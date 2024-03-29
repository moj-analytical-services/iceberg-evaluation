# settings and boilerplate code can go here (if its the same for all use cases)
 
import sys
import boto3
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, Row
from pyspark.context import SparkConf
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import time

def _getOptionalResolvedOptions(arguments):
    args_dict = {}
    for i in range(1, len(arguments), 2):
        arg_name = arguments[i].lstrip('--')
        arg_value = arguments[i + 1]
        args_dict[arg_name] = arg_value
    return args_dict
    
def bulk_insert(full_load_path,output_directory,future_end_datetime,spark):
    
    # read the bulk insert parquet file
    full_load=spark.read.parquet(full_load_path)
    # adds 3 new columns
    full_load = full_load.withColumn("start_datetime",F.col("extraction_timestamp"))
    full_load = full_load.withColumn("end_datetime", F.to_timestamp(F.lit(future_end_datetime), 'yyyy-MM-dd'))
    full_load = full_load.withColumn("op",F.lit(None).cast("string"))
    full_load = full_load.withColumn("is_current",F.lit(True))
    full_load.writeTo(output_directory).createOrReplace()
    #full_load.writeTo(output_directory).create()
    
    
    



    
    
def scd2_simple(updates_filepath, output_directory, future_end_datetime, primary_key,spark):
    table_name = "datagensb"
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


    
    
def scd2_complex(updates_filepath, output_directory, future_end_datetime, primary_key,spark):
    table_name = "datagensb"
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
    


if __name__ == "__main__":
    
    import sys
    import datetime
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions
    
    # args = _getOptionalResolvedOptions(sys.argv)
    # these are the arguments passed to the glue-job from step functions
    # you dont need to include them if you dont want
    args = getResolvedOptions(sys.argv, ["use_case",
                                         "bucket",
                                         "output_key_base",
                                         "table",
                                         "primary_key",
                                         "scale",
                                         "proportion",
                                         "scd2_type"
                                         ])

    use_case = args.get("use_case", "scd2_complex")
    bucket = args.get("bucket", "sb-test-bucket-ireland")
    output_key_base = args.get("output_key_base", "data-engineering-use-cases")
    table = args.get("table", "store_sales")
    primary_key = args.get("primary_key", "pk")
    scale = args.get("scale", 1)
    proportion = args.get("proportion", 0.001)
    str_proportion = str(proportion).replace(".", "_")
    scd2_type = args.get("scd2_type", "complex")
    
    compute = "glue_iceberg"
    # use_case = args.get("use_case","bulk_insert")
    # bucket = args.get("bucket")
    # output_key_base = args.get("output_key_base")
    # table = args.get("table")
    # primary_key = args.get("primary_key")
    # scale = args.get("scale")
    # proportion = args.get("proportion")
    # str_proportion = str(proportion).replace(".", "_")
    # scd2_type = args.get("scd2_type","simple")
    
    catalog_name = "glue_catalog"
    # use_case = "scd2_simple"
    # bucket = "sb-test-bucket-ireland"
    # output_key_base = "data-engineering-use-cases"
    # #s3://sb-test-bucket-ireland/data-engineering-use-cases/
    # table = "store_sales"
    # primary_key="pk"
    # scale=1
    # proportion=0.001
    # str_proportion = str(proportion).replace(".", "_")

    full_load_path = f"s3://{bucket}/tpcds/scale={scale}/table={table}"
    updates_filepath = f"s3://{bucket}/tpcds_updates/scale={scale}/table={table}/proportion={str_proportion}/"
    warehouse = f"s3://{bucket}/{output_key_base}/compute={compute}/"
    database_name = f"tpcds_{scale}"
    dest_database_name = f"tpcds_{scale}_{compute}"
    table_name = f"{table}_{str_proportion}_{scd2_type}"
    future_end_datetime = datetime.datetime(2250, 1, 1)
    output_directory = f"{catalog_name}.{dest_database_name}.{table_name}"
    #folder = "data-engineering-use-cases/compute=glue_iceberg/tpcds_1_glue_iceberg.db/store_sales_1_0_001_simple/"
    folder = f"{output_key_base}/compute={compute}/{dest_database_name}.db/{table_name}/"
    parallelism = 2000
    
    
    conf = SparkConf()
    conf.set("spark.shuffle.storage.path","s3://sb-test-bucket-ireland/shuffle-data/")
    conf.set("spark.default.parallelism", parallelism)
    conf.set("spark.sql.shuffle.partitions", parallelism)
    conf.set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set(f"spark.sql.catalog.{catalog_name}.warehouse", f"{warehouse}") 
    conf.set(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") 
    conf.set(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") 
    conf.set(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") 
    glue_context = GlueContext(SparkContext.getOrCreate(conf=conf))
    logger = glue_context.get_logger()
    logger.warn(f"{str(args)}")
    logger.warn("output_directory")
    spark = glue_context.spark_session
    
    
    # create the database
    query = f"""
    CREATE DATABASE IF NOT EXISTS {catalog_name}.{dest_database_name}
    """
    spark.sql(query)
    
    
    if use_case == "bulk_insert":
        
        # add any bulk_insert specific configs
        #Delete if table exists
        query = f"""
        DROP TABLE IF EXISTS {catalog_name}.{dest_database_name}.{table_name}
        """
        spark.sql(query)
        ## Delete files in S3
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(bucket)
        bucket.objects.filter(Prefix=folder).delete()
        _ = bulk_insert(full_load_path,output_directory,future_end_datetime)
    
    if use_case == "scd2_simple":
        
        # add any scd2_simple specific configs
    
        _ = scd2_simple(updates_filepath, output_directory, future_end_datetime, primary_key)
    
    if use_case == "scd2_complex":
        
        # add any scd2_simple specific configs
    
        _ = scd2_complex(updates_filepath, output_directory, future_end_datetime, primary_key)
