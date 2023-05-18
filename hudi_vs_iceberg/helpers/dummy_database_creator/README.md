# Creating a dummy database for iceberg and hudi using Glue

## Prerequisites

To run the dummy database creator locally you will need to:
* Assume an AWS role with the required permissions
* Make sure you have docker installed and pull down the official AWS Glue 4.0 docker image by running
`docker pull amazon/aws-glue-libs:glue_libs_4.0.0_image_01`
* Set an environment variable `WORKSPACE_LOCATION` to be the full path to the `dummy_database_creator` directory
* Zip the contents of the `src` folder to a zip file called `src.zip`

## Hudi

To create a dummy database using Hudi as the data lake format run:

```
docker run -it -v $WORKSPACE_LOCATION:/home/glue_user/workspace/ \
    -e AWS_DEFAULT_REGION=eu-west-1 \
    -e DISABLE_SSL=true \
    -e DATALAKE_FORMATS=hudi \
    -e AWS_REGION=eu-west-1 \
    -e AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN \
    -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    --rm -p 4040:4040 -p 18080:18080 \
    --name glue_pyspark_test4 amazon/aws-glue-libs:glue_libs_4.0.0_image_01 \
    spark-submit /home/glue_user/workspace/src/job.py \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.hive.convertMetastoreParquet=false
    --py-files src.zip
```

## Iceberg

To create a dummy database using Iceberg as the data lake format run:

```
docker run -it -v $WORKSPACE_LOCATION:/home/glue_user/workspace/ \
    -e AWS_DEFAULT_REGION=eu-west-1 \
    -e DISABLE_SSL=true \
    -e DATALAKE_FORMATS=iceberg \
    -e AWS_REGION=eu-west-1 \
    -e AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN \
    -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    --rm -p 4040:4040 -p 18080:18080 \
    --name glue_pyspark_test4 amazon/aws-glue-libs:glue_libs_4.0.0_image_01 \
    spark-submit /home/glue_user/workspace/src/job.py \
    --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.glue_catalog.warehouse=s3://alpha-everyone/datalake_formats_testing \
    --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
    --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    --py-files src.zip
```
