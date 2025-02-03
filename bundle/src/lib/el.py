from typing import Callable, Union, Dict
from pyspark.sql import DataFrame, Column
from databricks.sdk.runtime import spark
from delta.tables import DeltaTable
from abc import ABC, abstractmethod


class SqlConnectionData:
    """
    Encapsulates connection details required to connect to a SQL
    database.

    Attributes:
        host (str): The hostname or IP address of the SQL database server.
        port (str): The port number on which the SQL database server is
            listening.
        database (str): The name of the database to connect to.
        username (str): The username to authenticate with the SQL database.
        password (str): The password associated with the username.
    """

    def __init__(self, host: str, port: str, database: str,
                 username: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password


class DataLoader(ABC):
    """
    Members of this class can extract data from different sources and load
    them incrementally into delta tables.

    Attributes:
        table_name (str): The name of the table to be extracted.
        schema_name (str): The name of the schema which contains the table.
        primary_key (Union[list[str], Dict[str, str]]): A list or dictionary
            of columns that compose the primary key. If it's a dictionary,
            then each key represents the column name in the source table and
            each value represents the column name in the target table.
    """

    def __init__(
        self,
        schema_name: str,
        table_name: str,
        primary_key: Union[list[str], Dict[str, str]]
    ):
        self.df = None
        self.schema_name = schema_name
        self.table_name = table_name
        self.primary_key = primary_key

    def __build_merge_condition__(self):
        if isinstance(self.primary_key, dict):
            condition_list = [
                f"target.{v} = source.{k}" for k,
                v in self.primary_key.items()]
        else:
            condition_list = [
                f"target.{pk} = source.{pk}" for pk in self.primary_key]

        return " AND ".join(condition_list)

    def __merge_into__(self, target_table: str):
        if self.df is None:
            raise ValueError("Data Loader not loaded")

        delta_table = DeltaTable.forName(spark, target_table)

        return (delta_table.alias("target")
                .merge(
                self.df.alias("source"),
                self.__build_merge_condition__()
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
                )

    @staticmethod
    def fromDataFrame(df: DataFrame, *primary_key):
        """
        Creates the DataLoader from an existing spark DataFrame.

        Args:
            df (DataFrame): The spark DataFrame.

        Returns:
            DataLoader: The created DataLoader
        """
        dl = DataLoader(
            schema_name=None,
            table_name=None,
            primary_key=primary_key
        )

        dl.df = df

        return dl

    @abstractmethod
    def extract(self, filter):
        """
        To be implemented:
            Set self.df to a DataFrame created from the source table.

        Args:
            filter: The filter for the extraction, or None for no filtering.

        Returns:
            DataLoader
        """
        pass

    def apply(self, callable: Callable[[DataFrame], DataFrame]):
        """
        Transforms self.df based on a transformation function. To be used after
        extraction and before loading.

        Args:
            callable (Callable[[DataFrame], DataFrame]): Function that applies
                a transformation to a spark DataFrame.

        Returns:
           DataLoader
        """
        if self.df is None:
            raise ValueError("Data not extracted")

        self.df = callable(self.df)

        return self

    def load_into(self, target_table: str):
        """
        Loads extracted data into a target table.

        Args:
            target_table (str): The fully qualified name of the target table.

        Returns:
            DataLoader
        """
        if self.df is None:
            raise ValueError("Data not extracted")

        if self.df.count() == 0:
            return

        if spark.catalog.tableExists(target_table):
            self.__merge_into__(target_table)
        else:
            (self.df.write
                .format("delta")
                .saveAsTable(target_table)
             )

        return self


class MSSqlDataLoader(DataLoader):
    """
    DataLoader which sources data from SQL Server.

    Attributes:
        table_name (str): The name of the table to be extracted.
        schema_name (str): The name of the schema which contains the table.
        primary_key (Union[list[str], Dict[str, str]]): A list or dictionary
            of columns that compose the primary key. If it's a dictionary,
            then each key represents the column name in the source table and
            each value represents the column name in the target table.
        connection_data (SqlConnectionData): An instance of the
            SqlConnectionData class.
        selected (list[str]): A list of columns to select, or None for all
            columns.
    """

    def __init__(
        self,
        schema_name: str,
        table_name: str,
        primary_key: list[str],
        connection_data: SqlConnectionData,
        selected: list[str] = None,
    ):
        self.df = None
        self.connection_data = connection_data
        self.schema_name = schema_name
        self.table_name = table_name
        self.selected = selected
        self.primary_key = primary_key

    def __build_dbtable_query__(self, filter: str):
        select = "*" if self.selected is None else ", ".join(self.selected)
        filter = "1 = 1" if filter is None else filter

        return f"""
            (select {select}
            from {self.schema_name}.{self.table_name}
            where {filter}) as {self.table_name}
        """

    def extract(self, filter: str = None):
        """
        Extracts data from the SQL Server database table.

        Args:
            filter (str): A SQL expression used for filtering data, or None for
            no filtering.

        Returns:
            DataLoader
        """
        self.df = (spark.read.format("sqlserver")
                   .option("encrypt", False)
                   .option("host", self.connection_data.host)
                   .option("port", self.connection_data.port)
                   .option("user", self.connection_data.username)
                   .option("password", self.connection_data.password)
                   .option("database", self.connection_data.database)
                   .option("dbtable", self.__build_dbtable_query__(filter))
                   .load()
                   )

        return self


class DeltaDataLoader(DataLoader):
    """
    DataLoader which sources data from Delta tables.

    Attributes:
        table_name (str): The name of the table to be extracted.
        schema_name (str): The name of the schema which contains the table.
        primary_key (Union[list[str], Dict[str, str]]): A list or dictionary
            of columns that compose the primary key. If it's a dictionary,
            then each key represents the column name in the source table and
            each value represents the column name in the target table.
        selected (list[str]): A list of columns to select, or None for all
            columns.
        catalog_name (str): The name of the Unity Catalog catalog where the
            data is located.
    """

    def __init__(
        self,
        schema_name: str,
        table_name: str,
        primary_key: list[str],
        selected=None,
        catalog_name: str = None
    ):
        self.df = None
        self.schema_name = schema_name
        self.table_name = table_name
        self.selected = selected
        self.primary_key = primary_key
        self.catalog_name = catalog_name

    def extract(self, filter: Union[str, Column] = None):
        """
        Extracts data from the Delta table.

        Args:
            filter (str): A SQL expression or spark Column expression for
            filtering data, or None for no filtering.

        Returns:
            DataLoader
        """
        if self.catalog_name is None:
            name = f"{self.schema_name}.{self.table_name}"
        else:
            name = f"{self.catalog_name}.{self.schema_name}.{self.table_name}"

        self.df = spark.table(name)

        if self.selected is not None:
            self.df = self.df.select(self.selected)

        if filter is not None:
            self.df = self.df.where(filter)

        return self
