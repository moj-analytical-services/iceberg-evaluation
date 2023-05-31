import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext

glue_context = GlueContext(SparkContext())
spark = glue_context.spark_session


bucket_name = "sb-test-bucket-ireland"
scale = 3000
partitions = 36
database_name = f"tpcds_{scale}"
source_table = "store_sales"
path = f"tpcds_updates/scale={scale}/table={source_table}/"


# number of days updated (there are 1823 days altogether)
updated_days = [1]

for update in updated_days:
    
    table = f"{source_table}_{update}"
    
    ## Delete files in S3
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.objects.filter(Prefix=f"{path}updated_days={update}/").delete()
    
    ## Drop table in Glue Data Catalog
    try:
        glue = boto3.client('glue')
        glue.delete_table(DatabaseName=database_name, Name=table)
    except glue.exceptions.EntityNotFoundException:
        print(f"Table {database_name}.{table} does not exist")
    
    # Create a DynamicFrame for `table`
    dyf = glue_context.create_dynamic_frame.from_catalog(database=database_name, table_name=source_table)
    dyf_filter = dyf.filter( f=lambda x: x["ss_sold_date_sk"] in [2450816])

    # Write the DynamicFrame to S3 and register the table.
    sink = glue_context.getSink(
        connection_type="s3", 
        path=f"s3://{bucket_name}/{path}updated_days={update}/",
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE")   
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase=database_name, catalogTableName=table)
    sink.writeFrame(dyf_filter)
