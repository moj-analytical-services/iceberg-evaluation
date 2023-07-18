import pandas as pd
import pytest


def sort_df_by_column_name(df):
    df = df.reindex(sorted(df.columns), axis=1)
    return df


def order_by_row_key(df):
    ordered_df = df.sort_values(by=["pk", "extraction_timestamp"])
    return ordered_df


def drop_df_index(df):
    return df.reset_index(drop=True)


def prepare_df_for_assertion(df):
    df = sort_df_by_column_name(df)
    df = order_by_row_key(df)
    df = drop_df_index(df)
    return df


def assert_df_equality(expected_df, actual_df):
    expected_df = prepare_df_for_assertion(expected_df)
    actual_df = prepare_df_for_assertion(actual_df)
    pd.testing.assert_frame_equal(expected_df, actual_df, check_dtype=False)