from pyspark.sql import SparkSession


class SessionBuilder:
    @staticmethod
    def _build_iceberg(warehouse_path: str):
        spark_session = (
            SparkSession.builder.config(
                "spark.sql.catalog.glue_catalog",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config("spark.sql.catalog.glue_catalog.warehouse", f"{warehouse_path}")
            .config(
                "spark.sql.catalog.glue_catalog.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog",
            )
            .config(
                "spark.sql.catalog.glue_catalog.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .getOrCreate()
        )
        return spark_session

    @staticmethod
    def _build_hudi():
        spark_session = (
            SparkSession.builder.config(
                "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
            )
            .config("spark.sql.hive.convertMetastoreParquet", "False")
            .getOrCreate()
        )
        return spark_session

    @staticmethod
    def _build_parquet():
        return SparkSession.builder.getOrCreate()

    @classmethod
    def build(cls, format: str, **kwargs):
        format_method = getattr(cls, f"_build_{format}")
        return format_method(**kwargs)
