import os
import re
from pathlib import Path
from typing import Optional, Tuple, Union

import awswrangler as wr
import pandas as pd


def get_query_stats(
    query_filepath: str, database_name: str, total_iterations: Optional[int] = 10
) -> pd.DataFrame:
    with open(query_filepath, "r") as f:
        query = f.read()

    results = []
    for _ in range(total_iterations):
        query_id = wr.athena.start_query_execution(query, database=database_name)
        resp = wr.athena.wait_query(query_id)

        if resp["Status"]["State"] == "SUCCEEDED":
            stats = resp["Statistics"]
            _ = stats.pop("ResultReuseInformation")
            results.append(stats)

        else:
            raise wr.exceptions.QueryFailed("Query failed to execute")

    ds = pd.DataFrame(data=results)
    df = pd.DataFrame(ds.mean(), columns=[Path(query_filepath).stem])

    return df


def get_all_query_stats(
    database_name: str,
    sql_directory: Optional[str] = "sql",
    limit: Optional[Union[Tuple[int, int], None]] = None,
    total_iterations: Optional[int] = 10,
) -> pd.DataFrame:
    sql_files = sorted(
        os.listdir(sql_directory),
        key=lambda x: int(re.sub("[a-z.]", "", x)),
    )
    if limit is not None:
        min, max = limit
        sql_files = sql_files[min:max]

    results = []
    for sql_file in sql_files:
        sql_query_path = f"{sql_directory}/{sql_file}"
        try:
            result = get_query_stats(
                sql_query_path,
                database_name=database_name,
                total_iterations=total_iterations,
            )
            results.append(result)
        except Exception as e:
            print(f"Query {sql_file} failed:\n{str(e)}")

    results_df = pd.concat(results, axis=1)

    return results_df
