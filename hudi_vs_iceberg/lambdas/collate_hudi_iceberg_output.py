import datetime
import json
import boto3


def flatten_dict(d: dict) -> dict:
    """
    where values in a dict are a dict themselve bring them up a level
    works recursively to create a 'flat' dictionary

    note:
        - keys where the value is a dictionary will be lost
        - where keys are repeated in a nested dict, the deepest value will be the one which is
          preserved
    """
    o = dict()
    for k, v in d.items():
        if isinstance(v, dict):
            o = {**o,
                 **flatten_dict(v)}
        else:
            o[k] = v
    return o


def format_dict(d: dict) -> dict:
    """flatten dict and remove '--' prefix from keys"""
    if d:
        d = flatten_dict(d)
        d = {k.lstrip("--"): v for k, v in d.items()}
    return d


def collate_format_output(output: dict) -> list[dict]:
    """
    takes the output of the hudi_vs_iceberg state-machine and returns a list of dicts with the
    results for each update proportion, compute and use-case tested
    """
    # these are the potential steps we want to measure
    keys = ["bulk_insert_simple", "bulk_insert_complex", "simple_scd2", "scd2_complex"]

    results = []
    for proportion_results in output["proportion_results"]:
        for compute in proportion_results["test_computes"]:
            results.extend([format_dict(compute.get(k)) for k in keys])

    results = [r for r in results if r]
    return results


def lambda_handler(event, context):
    results = collate_format_output(event)

    bucket = event["bucket"]
    output_key_base = event["output_key_base"]
    timestamp = datetime.datetime.now()
    filename = f"hudi_vs_iceberg_{timestamp.strftime('%Y%m%d%H%M%S')}.json"
    key = f"{output_key_base}/step_function_results/{filename}"

    s3 = boto3.client("s3")

    s3.put_object(Body=json.dumps(results),
                  Bucket=bucket,
                  Key=key)

    return {
        'statusCode': 200,
        'body': results
    }

