import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext

glue_context = GlueContext(SparkContext())
spark = glue_context.spark_session


bucket_name = "sb-test-bucket-ireland"
scale = 3000
partitions = 36
database_name = f"tpcds_{scale}"

tables = [
    # "call_center", "catalog_returns", 
    # "catalog_page", "catalog_sales",
    # "customer", "customer_address", "customer_demographics",
    # "date_dim", "dbgen_version", "household_demographics",
    # "income_band", "inventory", "item", "promotion", "reason",
    # "ship_mode", "store", "store_returns", 
    "store_sales",
    # "time_dim", "warehouse",
    # "web_page", "web_returns", "web_sales", "web_site"
    ]


## Create a database if not exists
try:
    glue = boto3.client('glue')
    glue.create_database(DatabaseInput={'Name': database_name})
    print(f"New database {database_name} created")
except glue.exceptions.AlreadyExistsException:
    print(f"Database {database_name} already exist")
    

for table in tables:
    
    ## Delete files in S3
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.objects.filter(Prefix=f"tpcds/scale={scale}/table={table}/").delete()
    
    ## Drop table in Glue Data Catalog
    try:
        glue = boto3.client('glue')
        glue.delete_table(DatabaseName=database_name, Name=table)
    except glue.exceptions.EntityNotFoundException:
        print(f"Table {database_name}.{table} does not exist")
    
    # Create a DynamicFrame for `table`
    dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="marketplace.spark",
        connection_options={
            "table": table,
            "scale": scale,
            "numPartitions": partitions,
            "connectionName": "tpcds-connector-glue3"},
        transformation_ctx="dyf")
    
    # Write the DynamicFrame to S3 and register the table.
    sink = glue_context.getSink(
        connection_type="s3", 
        path=f"s3://{bucket_name}/tpcds/scale={scale}/table={table}/",
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE")   
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase=database_name, catalogTableName=table)
    sink.writeFrame(dyf)
