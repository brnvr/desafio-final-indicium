from pyspark.sql import DataFrame
from lib.naming import pascal_to_snake


def column_names_to_snakecase(df: DataFrame):
    """
    Conforms all column names in DataFrame to snake_case.

    Args:
        df (DataFrame): The DataFrame to be transformed.a

    Returns:
        DataFrame
    """
    for col in df.columns:
        df = df.withColumnRenamed(col, pascal_to_snake(col))

    return df


def column_names_renamed(
    df: DataFrame,
    columns_prev: list[str],
    columns_new: list[str]
):
    """
    Renames all column names in columns_prev to names in columns_new.

    Args:
        df (DataFrame): The DataFrame to be transformed.
        columns_prev (list[str]): List of column names to be renamed
        columns_new (list[str]): List of new column names. It must have the
            same length as columns_prev.

    Returns:
        DataFrame
    """
    if columns_new is None:
        return df

    if len(columns_prev) != len(columns_new):
        raise ValueError(
            "columns_prev and columns_new must have the same length.")

    dict = {}

    for i in range(0, len(columns_prev)):
        dict[columns_prev[i]] = columns_new[i]

    return df.withColumnsRenamed(dict)
