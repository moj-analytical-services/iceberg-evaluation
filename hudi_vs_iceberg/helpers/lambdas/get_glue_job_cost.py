import boto3


def get_job_cost(glue_client: boto3.client,
                 glue_job_name: str,
                 glue_job_id: str,
                 dpu_hour_cost: str=0.44) -> dict:
    """
    for a given glue-job-run calculate the cost and return with other run-statistics

    Args:
        glue_client (boto3.client): client for aws glue
        glue_job_name (str): the name of the glue-job to get cost/stats for
        glue_job_id (str): the id of the glue-job to get cost/stats for
        dpu_hour_cost (str, optional): the cost per dpu_hour for a glue job, as set by aws.
            Defaults to 0.44

    Returns:
        dict: {
            "num_workers": the of workers which ran the glue job,
            "execution_time": the execution time of the glue job,
            "dpu_seconds": the dpu-seconds used by the glue job,
            "cost": the cost of the glue-job in dollars
        }
    """

    worker_multiplier = {"G.1X": 1,
                         "G.2X": 2}

    r = glue_client.get_job_run(JobName=glue_job_name,
                                RunId=glue_job_id,
                                PredecessorsIncluded=False)

    execution_time = r["JobRun"].get("ExecutionTime")
    worker_type = r["JobRun"].get("WorkerType")
    num_workers = r["JobRun"].get("NumberOfWorkers")

    dpu_seconds = r["JobRun"].get("DPUSeconds")

    # dpu_seconds are only returned for auto-scaling jobs else we need to calculate
    if not dpu_seconds:
        dpu_seconds = execution_time * num_workers * worker_multiplier.get(worker_type)

    cost = (dpu_seconds / 3600) * dpu_hour_cost

    return {
        "num_workers": num_workers,
        "execution_time": execution_time,
        "dpu_seconds": dpu_seconds,
        "cost": cost,
    }


def lambda_handler(event, context):

    glue_client = boto3.client("glue")

    costs = get_job_cost(glue_client, **event)

    return {
        'statusCode': 200,
        'body': {**costs,
                 **event},
    }
