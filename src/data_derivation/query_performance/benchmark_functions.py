import os
import re
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import awswrangler as wr
import boto3.session
import pandas as pd


def get_query_stats(
    query: str,
    query_name: str,
    database_name: str,
    total_iterations: Optional[int] = 10,
) -> Tuple[str, List[Dict[str, str]]]:
    session = boto3.session.Session()

    results = []
    for _ in range(total_iterations):
        query_id = wr.athena.start_query_execution(
            query, database=database_name, boto3_session=session
        )
        resp = wr.athena.wait_query(query_id, boto3_session=session)

        if resp["Status"]["State"] == "SUCCEEDED":
            stats = resp["Statistics"]
            _ = stats.pop("ResultReuseInformation")
            results.append(stats)

        else:
            raise wr.exceptions.QueryFailed("Query failed to execute")

    return query_name, results


def future_exception_handler(future: Future, handle_type="warning"):
    try:
        result = future.result()
        return result
    except Exception as exc:
        if handle_type == "error":
            print(f"future generated an exception: {exc}")
            raise
        elif handle_type == "warning":
            print(f"future generated an exception: {exc}")
            return None


def convert_query_results_to_df(
    query_name: str, query_results: List[Dict[str, str]]
) -> pd.DataFrame:
    ds = pd.DataFrame(data=query_results)
    return pd.DataFrame(ds.mean(), columns=[query_name])


def query_ordering(query: str) -> int:
    return int(re.sub("[a-z.]", "", query))


def get_all_query_stats(
    database_name: str,
    sql_directory: Optional[str] = "sql",
    limit: Optional[Union[Tuple[int, int], None]] = None,
    total_iterations: Optional[int] = 10,
) -> pd.DataFrame:
    sql_files = sorted(
        os.listdir(sql_directory),
        key=query_ordering,
    )

    if limit is not None:
        min, max = limit
        sql_files = sql_files[min:max]

    query_arguments = []
    for sql_file in sql_files:
        query_path = f"{sql_directory}/{sql_file}"
        query_name = Path(sql_file).stem

        with open(query_path, "r") as f:
            query = f.read()

        query_arguments.append((query_name, query))

    with ThreadPoolExecutor() as ex:
        results = [
            ex.submit(
                get_query_stats, query, query_name, database_name, total_iterations
            )
            for query_name, query in query_arguments
        ]

        values = [future_exception_handler(future) for future in as_completed(results)]

    dfs = [
        convert_query_results_to_df(query_name, query_results)
        for query_name, query_results in values
    ]
    results_df = pd.concat(dfs, axis=1)
    results_ordered_df = results_df.reindex(
        columns=sorted(results_df.columns, key=query_ordering)
    )

    return results_ordered_df
