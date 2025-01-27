from pyspark.sql import DataFrame
from lib.naming import pascal_to_snake


def column_names_to_snakecase(df: DataFrame):
    for col in df.columns:
        df = df.withColumnRenamed(col, pascal_to_snake(col))

    return df


def primary_key_column_names_renamed(
        df: DataFrame, primary_key_prev: list[str], primary_key_new: list[str]):
    if primary_key_new is None:
        return df

    if len(primary_key_prev) != len(primary_key_new):
        raise ValueError(
            "primary_key_prev and primary_key_new must have the same length.")

    dict = {}

    for i in range(0, len(primary_key_prev)):
        dict[primary_key_prev[i]] = primary_key_new[i]

    return df.withColumnsRenamed(dict)
