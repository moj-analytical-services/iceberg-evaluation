import os
from typing import Dict

import yaml


def get_target_attributes_from_config(config: Dict) -> Dict:
    target_config = config["target_list"][0]
    return target_config["attributes"]


def construct_warehouse_path_from_config(config: Dict) -> str:
    target_attributes = get_target_attributes_from_config(config)
    bucket_name = target_attributes["bucket_name"]
    bucket_prefix = target_attributes["bucket_prefix"]
    return f"s3://{bucket_name}/{bucket_prefix}"


def validate_set_format_against_config(set_format: str, config: Dict):
    if len(set_format.split(",")) != 1:
        raise ValueError(
            "Only one of either hudi or iceberg may be specified in "
            + "DATALAKE_FORMATS"
        )

    target_attributes = get_target_attributes_from_config(config)
    config_format = target_attributes["format"]

    if set_format != "":
        if config_format != set_format:
            raise ValueError(
                f"Config format {config_format} should match "
                + f"DATALAKE_FORMATS {set_format}"
            )
    else:
        if config_format in ["hudi", "iceberg"]:
            raise ValueError(
                "DATALAKE_FORMATS environment variable should "
                + f"be set to {config_format}"
            )


def get_config(set_format: str, config_filename: str) -> Dict:
    if config_filename != "":
        path = os.path.join("src/config", config_filename)
    elif set_format != "":
        path = f"src/config/{set_format}_config.yml"
    else:
        raise ValueError("One of config_filename or set_format should be set")

    with open(path, "r") as f:
        config_file = yaml.safe_load(f)

    return config_file
