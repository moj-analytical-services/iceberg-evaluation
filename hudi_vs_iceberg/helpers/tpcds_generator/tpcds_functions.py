import boto3
import pyspark.sql.functions as f
from awsglue.dynamicframe import DynamicFrame


## Create a database if not exists
def create_db_if_not_exists(database_prefix=f"tpcds_", scale=1):
    database_name = f"{database_prefix}{scale}"
    try:
        glue = boto3.client("glue")
        glue.create_database(DatabaseInput={"Name": database_name})
        print(f"New database {database_name} created")
    except glue.exceptions.AlreadyExistsException:
        print(f"Database {database_name} already exist")


def generate_tables(
    glue_context,
    tables=["store_sales"],
    bucket_name="sb-test-bucket-ireland",
    folder_name="tpcds",
    database_prefix=f"tpcds_",
    scale=1,
    partitions=36,
    extraction_timestamp="2022-01-01",
):
    database_name = f"{database_prefix}{scale}"
    for table in tables:
        ## Delete files in S3
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(bucket_name)
        bucket.objects.filter(
            Prefix=f"{folder_name}/scale={scale}/table={table}/"
        ).delete()

        ## Drop table in Glue Data Catalog
        try:
            glue = boto3.client("glue")
            glue.delete_table(DatabaseName=database_name, Name=table)
            print(f"Table {database_name}.{table} dropped")
        except glue.exceptions.EntityNotFoundException:
            print(f"Table {database_name}.{table} does not exist")

        # Create a DynamicFrame for `table`
        dyf = glue_context.create_dynamic_frame.from_options(
            connection_type="marketplace.spark",
            connection_options={
                "table": table,
                "scale": scale,
                "numPartitions": partitions,
                "connectionName": "tpcds-connector-glue3",
            },
            transformation_ctx="dyf",
        )
        
        df = (
            dyf.toDF()
            .withColumn(
                "extraction_timestamp",
                f.to_timestamp(f.lit(extraction_timestamp), "yyyy-MM-dd"),
            )
            .withColumn("op", f.lit(None).cast("string"))
        )
        if table == "store_sales":
            # create single pk column
            df = df.withColumn(
                    "pk",
                    f.concat(
                        f.col("ss_ticket_number").cast("string"),
                        f.lit("-"),
                        f.col("ss_item_sk").cast("string"),
                    ),
                )
        dyf2 = DynamicFrame.fromDF(df, glue_context, "dyf2")

        # Write the DynamicFrame to S3 and register the table.
        sink = glue_context.getSink(
            connection_type="s3",
            path=f"s3://{bucket_name}/{folder_name}/scale={scale}/table={table}/",
            enableUpdateCatalog=True,
            updateBehavior="UPDATE_IN_DATABASE",
        )
        sink.setFormat("glueparquet")
        sink.setCatalogInfo(catalogDatabase=database_name, catalogTableName=table)
        sink.writeFrame(dyf2)
        print(f"Table {database_name}.{table} generated")
