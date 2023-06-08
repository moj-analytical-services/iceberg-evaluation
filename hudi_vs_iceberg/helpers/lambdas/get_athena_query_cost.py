import json
import boto3


def load_stats(glue_client: boto3.client, bucket: str, key: str, filename: str) -> list[dict]:
    """
    load a json file containing info on one or more athena queries from an s3 location,
    return the Statistics node for each query

    Args:
        glue_client (boto3.client): client for aws s3
        bucket (str): name of the bucket containing the file
        key (str): the key/location in the bucket of the file
        filename (str): the name the file (without file extension)

    Returns:
        list[dict]: the `Statistics` for each athena query in given file
    """

    obj = glue_client.get_object(Bucket=bucket, Key=f"{key}{filename}.json")
    results = json.loads(obj.get("Body").read())
    stats = [q.get("Statistics") for q in results]

    return stats


def get_query_cost(stats: list[dict], cost_per_tb: float=5.0) -> dict:
    """
    given the statistics of a list of athena-queries return the total cost and associate metrics

    Args:
        stats (list[dict]): the `Statistics` node for one or more athena queries, as returned by
            [GetQueryExecution](https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryExecution.html)
        cost_per_tb (float, optional): the cost (in dollars), per terrabyte-scanned of an athena
            query as set by aws. Defaults to 5.0.

    Returns:
        dict: _description_
    """

    bytes_per_tb = 2**40

    execution_time = sum([i["TotalExecutionTimeInMillis"] for i in stats]) / 1000
    tb_scanned = sum([i["DataScannedInBytes"] for i in stats]) / bytes_per_tb
    cost = tb_scanned * cost_per_tb

    return {
        "execution_time": execution_time,
        "tb_scanned": tb_scanned,
        "cost": cost
    }


def lambda_handler(event, context):
    """
    this lambda function reads the statistics of one or more athena queries from a json stored
    in s3 and calcultes the total execution_time, terrabytes-scanned and cost (in dollars)
    """

    s3 = boto3.client("s3")

    args = event.get("Arguments")

    response_path =  "/compute=athena_iceberg/responses/"
    key = f"{args['--output_key_base']}{response_path}"

    use_case = args["--use_case"]
    scd2_type = args["--scd2_type"]
    table = args["--table"]
    scale = args["--scale"]
    proportion = str(args["--proportion"]).replace(".", "_")

    filename = f"{use_case}_{scd2_type}_{table}_{scale}_{proportion}"

    stats = load_stats(
        glue_client=s3,
        bucket=args["--bucket"],
        key=key,
        filename=filename
        )

    costs = get_query_cost(stats)

    return {
        'statusCode': 200,
        'body': {**costs,
                 **event}
    }
