import boto3
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import pyspark.sql.functions as f
from awsglue.dynamicframe import DynamicFrame

glue_context = GlueContext(SparkContext())
spark = glue_context.spark_session

args = getResolvedOptions(
    sys.argv,
    [
        "bucket",
        "table",
        "scale",
        "proportion",
    ],
)

bucket_name = args.get("bucket", "sb-test-bucket-ireland")
source_table = args.get("table", "store_sales")
scale = args.get("scale", 1)
proportion = float(args.get("proportion", 0.001))

str_proportion = str(proportion).replace(".", "_")
extraction_timestamp = "2022-01-02"
database_name = f"tpcds_{scale}"
table = f"{source_table}_{str_proportion}"
table_prefix = f"tpcds_updates/scale={scale}/table={source_table}/proportion={str_proportion}/"

logger = glue_context.get_logger()
logger.warn(f"{str(args)}")
logger.warn(f"{table=}")
logger.warn(f"{table_prefix=}")


## Delete files in S3
s3 = boto3.resource("s3")
bucket = s3.Bucket(bucket_name)
bucket.objects.filter(Prefix=table_prefix).delete()

## Drop table in Glue Data Catalog
try:
    glue = boto3.client("glue")
    glue.delete_table(DatabaseName=database_name, Name=table)
except glue.exceptions.EntityNotFoundException:
    print(f"Table {database_name}.{table} does not exist")

df = glue_context.create_data_frame.from_catalog(
    database=database_name,
    table_name=source_table,
    additional_options={"useCatalogSchema": True},
)
df = df.sample(False, fraction=proportion, seed=1)
df = df.withColumn(
    "extraction_timestamp",
    f.to_timestamp(f.lit(extraction_timestamp), "yyyy-MM-dd"),
)
df = df.withColumn("op", f.lit("U").cast("string"))
df = df.withColumn("ss_quantity", f.lit(1).cast("integer"))

dyf2 = DynamicFrame.fromDF(df, glue_context, "dyf2")

# Write the DynamicFrame to S3 and register the table.
sink = glue_context.getSink(
    connection_type="s3",
    path=f"s3://{bucket_name}/{table_prefix}",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
)
sink.setFormat("glueparquet")
sink.setCatalogInfo(catalogDatabase=database_name, catalogTableName=table)
sink.writeFrame(dyf2)
