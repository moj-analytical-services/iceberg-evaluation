import boto3
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import pyspark.sql.functions as f
from awsglue.dynamicframe import DynamicFrame

glue_context = GlueContext(SparkContext())
spark = glue_context.spark_session


def _getOptionalResolvedOptions(arguments):
    args_dict = {}
    for i in range(1, len(arguments), 2):
        arg_name = arguments[i].lstrip("--")
        arg_value = arguments[i + 1]
        args_dict[arg_name] = arg_value
    return args_dict


args = _getOptionalResolvedOptions(sys.argv)

bucket_name = args.get("bucket", "sb-test-bucket-ireland")
source_table = args.get("table", "store_sales")
scale = args.get("scale", 1)
proportion = args.get("proportion", 0.001)
str_proportion = str(proportion).replace(".", "_")
extraction_timestamp = "2022-01-02"
database_name = f"tpcds_{scale}"
table = f"{source_table}_{str_proportion}"
path = f"tpcds_updates/scale={scale}/table={source_table}/proportion={str_proportion}/"

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

df = glue_context.create_data_frame.from_catalog(
    database=database_name,
    table_name=source_table,
    additional_options={"useCatalogSchema": True},
)
df = df.sample(False, fraction=0.001, seed=1)
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
    path=f"s3://{bucket_name}/{path}",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
)
sink.setFormat("glueparquet")
sink.setCatalogInfo(catalogDatabase=database_name, catalogTableName=table)
sink.writeFrame(dyf2)
