import pandas as pd
import time, datetime


def bulk_insert(input_filepath, output_directory, future_end_datetime):
    start_time = time.time()
    df = pd.read_parquet(input_filepath)
    df["start_datetime"] = df["extraction_timestamp"]
    df["end_datetime"] = future_end_datetime
    df["is_current"] = True

    output_filepath = f"{output_directory}bulk_insert.parquet"
    df.to_parquet(output_filepath)
    process_time = time.time() - start_time
    print(f"Output saved to {output_filepath} in {process_time}")
    return output_filepath


def scd2_simple(
    input_filepath, updates_filepath, output_directory, future_end_datetime, primary_key
):
    start_time = time.time()

    input = pd.read_parquet(input_filepath)
    updates = pd.read_parquet(updates_filepath)

    df = pd.merge(
        input,
        updates[[primary_key, "extraction_timestamp"]],
        on=primary_key,
        suffixes=(None, "_y"),
    )
    df["end_datetime"] = df["extraction_timestamp_y"]
    df.drop(columns=["extraction_timestamp_y"], inplace=True)
    df["is_current"] = False

    updates["start_datetime"] = updates["extraction_timestamp"]
    updates["end_datetime"] = future_end_datetime
    updates["is_current"] = True

    output = pd.concat([df, updates], ignore_index=True)

    output_filepath = f"{output_directory}scd2_simple.parquet"
    output.to_parquet(output_filepath)
    process_time = time.time() - start_time
    print(f"Output saved to {output_filepath} in {process_time}")
    return output_filepath


def scd2_complex(
    input_filepath, updates_filepath, output_directory, future_end_datetime, primary_key
):
    start_time = time.time()

    input = pd.read_parquet(input_filepath)
    updates = pd.read_parquet(updates_filepath)

    df = pd.concat([input, updates], ignore_index=True)
    df.sort_values(
        by=[primary_key, "extraction_timestamp"], ignore_index=True, inplace=True
    )
    df["end_datetime"] = df.groupby([primary_key])["extraction_timestamp"].shift(
        -1, fill_value=future_end_datetime
    )
    df["is_current"] = df["end_datetime"].apply(
        lambda x: True if x == future_end_datetime else False
    )
    df["start_datetime"] = df["extraction_timestamp"]

    output_filepath = f"{output_directory}scd2_complex.parquet"
    df.to_parquet(output_filepath)
    process_time = time.time() - start_time
    print(f"Output saved to {output_filepath} in {process_time}")
    return output_filepath
