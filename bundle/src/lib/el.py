from typing import Callable
from pyspark.sql import DataFrame
from databricks.sdk.runtime import *
from delta.tables import DeltaTable

class SqlConnectionData:
    def __init__(self, host, port, database, username, password):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password

class DataLoader:
    def __init__(
        self,
        schema_name: str,
        table_name: str,
        primary_key: list[str],
        selected = None
    ):
        self.df = None
        self.schema_name = schema_name
        self.table_name = table_name
        self.primary_key = primary_key  
        self.selected = selected      

    def __build_merge_condition__(self):
        condition_list = [f"target.{pk} = source.{pk}" for pk in self.primary_key]

        return " AND ".join(condition_list)
    
    def __merge_into__(self, target_table:str):
        if self.df is None:
            raise ValueError("Data Loader not loaded")

        delta_table = DeltaTable.forName(spark, target_table)

        print(self.__build_merge_condition__())

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
    def fromDataFrame(df, *primary_key):
        dl = DataLoader(
            schema_name = None,
            table_name = None,
            primary_key = primary_key
        )

        dl.df = df

        return dl
    
    def apply(self, callable:Callable[[DataFrame], DataFrame]):
        self.df = callable(self.df)

        return self

    def select(self, *selected):
        self.selected = selected

        return self

    def load_into(self, target_table:str):
        if self.df is None:
            raise ValueError("Data not extracted")
        
        if spark.catalog.tableExists(target_table):
            self.__merge_into__(target_table)
        else:
            (self.df.write
                .format("delta")
                .saveAsTable(target_table)
            )

        return self

class MSSqlDataLoader(DataLoader):
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

    def __build_dbtable_query__(self, filter:str):
        select = "*" if self.selected is None else ", ".join(self.selected)
        filter = "1 = 1" if filter is None else filter

        return f"(select {select} from {self.schema_name}.{self.table_name} where {filter}) as {self.table_name}"

    def extract(self, filter:str = None):
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
    def __init__(
        self,
        schema_name: str,
        table_name: str,
        primary_key: list[str],
        selected = None,
        catalog_name: str = None
    ):
        self.df = None
        self.schema_name = schema_name
        self.table_name = table_name
        self.selected = selected
        self.primary_key = primary_key
        self.catalog_name = catalog_name

    def extract(self, filter = None):
        if self.catalog_name is None:
            table_full_name = f"{self.schema_name}.{self.table_name}"
        else:
            table_full_name = f"{self.catalog_name}.{self.schema_name}.{self.table_name}"

        if filter is None:
            self.df = spark.table(table_full_name)
            
        else:
            self.df = spark.table(table_full_name).where(filter)

        return self