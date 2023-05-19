from typing import List

import boto3
from awsglue.dynamicframe import DynamicFrame

GENERATOR_TYPE_MAPPING = {
    "key_generator": "string",
    "string_generator": "string",
    "child_key_generator": "string",
    "float_generator": "float",
    "date_generator": "date",
    "close_date_generator": "date",
    "ip_address_generator": "string",
}


class TestDataGeneratorTarg:
    def __init__(self, glueContext):
        self.glueContext = glueContext
        self.spark = glueContext.spark_session

    def S3(self, df, descriptor, **kwargs):
        try:
            s3format = getattr(self, f'_S3{descriptor["format"]}')
            s3format(df, descriptor, **kwargs)

        except AttributeError:
            raise NotImplementedError(
                "Not implemented for formats other than hudi and iceberg"
            )

    def _S3parquet(self, df, descriptor):
        database_name = descriptor["database_name"]
        table_name = descriptor["table_name"]
        partition = descriptor["partition"]
        bucket_name = descriptor["bucket_name"]
        bucket_prefix = descriptor["bucket_prefix"]
        data_path = f"s3://{bucket_name}/{bucket_prefix}/{database_name}/{table_name}"

        dyF = DynamicFrame.fromDF(df, self.glueContext, "dynamic_frame_conversion")

        sink = self.glueContext.getSink(
            connection_type="s3",
            path=data_path,
            enableUpdateCatalog=True,
            updateBehavior="UPDATE_IN_DATABASE",
            partitionKeys=[partition],
        )

        sink.setFormat("parquet")
        sink.setCatalogInfo(catalogDatabase=database_name, catalogTableName=table_name)
        sink.writeFrame(dyF)

    def _S3hudi(self, df, descriptor):
        df.write.format("org.apache.hudi").options(**descriptor["options"]).mode(
            descriptor["mode"]
        ).save()

    def _S3iceberg(self, df, descriptor, attributes_list):
        database_name = descriptor["database_name"]
        table_name = descriptor["table_name"]
        bucket_name = descriptor["bucket_name"]
        bucket_prefix = descriptor["bucket_prefix"]
        partition = descriptor["partition"]

        table_query = self._convert_attributes_to_create_table_sql(
            bucket_name=bucket_name,
            bucket_prefix=bucket_prefix,
            database_name=database_name,
            table_name=table_name,
            partition=partition,
            attributes_list=attributes_list,
        )
        self.spark.sql(table_query)

        df.sortWithinPartitions(descriptor["partition"]).writeTo(
            f"glue_catalog.{database_name}.{table_name}"
        ).append()

    def cleanup_existing_resources(self, descriptor):
        try:
            format_cleanup = getattr(
                self, f'_cleanup_existing_resources_{descriptor["format"]}'
            )
            format_cleanup(descriptor)

        except AttributeError:
            raise NotImplementedError(
                "Not implemented for formats other than hudi and iceberg"
            )

    def _cleanup_existing_resources_iceberg(self, descriptor):
        self._cleanup_existing_resources(descriptor)

    def _cleanup_existing_resources_hudi(self, descriptor):
        self._cleanup_existing_resources(descriptor)

    def _cleanup_existing_resources_parquet(self, descriptor):
        self._cleanup_existing_resources(descriptor)

    def _cleanup_existing_resources(self, descriptor):
        database_name = descriptor["database_name"]
        table_name = descriptor["table_name"]
        bucket_name = descriptor["bucket_name"]
        bucket_prefix = descriptor["bucket_prefix"]

        table_prefix = f"{bucket_prefix}/{database_name}/{table_name}"

        # Create a database to host tables if not exists.
        try:
            glue = boto3.client("glue")
            glue.create_database(DatabaseInput={"Name": database_name})
            print(f"New database {database_name} created")

        except glue.exceptions.AlreadyExistsException:
            print(f"Database {database_name} already exist")

        # Delete files in S3
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(bucket_name)
        bucket.objects.filter(Prefix=f"{table_prefix}/").delete()

        # Drop table in Glue Data Catalog
        try:
            glue = boto3.client("glue")
            glue.delete_table(DatabaseName=database_name, Name=table_name)

        except glue.exceptions.EntityNotFoundException:
            print(f"Table {database_name}.{table_name} does not exist")

    @staticmethod
    def _generate_field_sql_clause(attribute) -> str:
        column_name = attribute["ColumnName"].lower()
        generator = attribute["Generator"]
        dtype = GENERATOR_TYPE_MAPPING[generator]
        sql_clause = f"{column_name} {dtype}"
        return sql_clause

    @classmethod
    def _convert_attributes_to_create_table_sql(
        cls,
        bucket_name: str,
        bucket_prefix: str,
        database_name: str,
        table_name: str,
        partition: str,
        attributes_list: List[str],
    ) -> str:
        fields_clause = ",\n    ".join(
            [cls._generate_field_sql_clause(attr) for attr in attributes_list]
        )

        table_sql = f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.{database_name}.{table_name} (
            {fields_clause}
        )
        USING iceberg
        PARTITIONED BY ({partition})
        location 's3://{bucket_name}/{bucket_prefix}/{database_name}/{table_name}'
        """

        return table_sql
