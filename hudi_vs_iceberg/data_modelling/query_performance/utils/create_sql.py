import os

import requests

endpoint = os.path.join(
    "https://raw.githubusercontent.com/awslabs/aws-athena-query-federation",
    "master/athena-tpcds/src/main/resources/queries/",
)

for i in range(1, 100):
    sql_endpoint = endpoint + f"q{i}.sql"
    filepath = f"sql/q{i}.sql"
    response = requests.get(sql_endpoint)

    if response.status_code > 200:
        for suffix in ["a", "b"]:
            filepath = f"sql/q{i}{suffix}.sql"
            sql_endpoint = endpoint + f"q{i}{suffix}.sql"
            response = requests.get(sql_endpoint)
            if response.status_code > 200:
                print(f"Failed to retrieve query {i}")

            else:
                text = response.text.replace("[ TABLE_SUFFIX ]", "")
                with open(filepath, "w") as f:
                    f.write(text)

    else:
        text = response.text.replace("[ TABLE_SUFFIX ]", "")
        with open(filepath, "w") as f:
            f.write(text)
