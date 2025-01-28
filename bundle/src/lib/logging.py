from databricks.sdk.runtime import spark
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructField, StringType, IntegerType, StructType


class Log:
    """
    Log information from data movements.

    Attributes:
        catalog_name (str): The Name of the Unity Catalog catalog where the
            target table is located.
        schema_name (str): The schema name.
        table_name (str): The table name.
        movements (int): The number of movements, i.e., rows ingested by the
            source table.
        error (str): An error message, if the ingestion failed, or None, if it
        was successful.
    """
    def __init__(self, catalog_name: str, schema_name: str,
                 table_name: str, movements: int, error: str = None):
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.error = error
        self.movements = movements


class Logger:
    """
    Provides an interface to log data movements.

    Attributes:
       table_name (str): The fully qualified name of the logs table.
    """
    def __init__(self, table_name: str):
        self.table_name = table_name

    SCHEMA = StructType([
        StructField("catalog_name", StringType(), False),
        StructField("schema_name", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("error", StringType(), True),
        StructField("movements", IntegerType(), False),
    ])

    def log(self, logs: list[Log]):
        """
        Logs data movements.

        Args:
            logs (list[Log]): A list of data movement logs.
        """
        data = [
            (log.catalog_name,
             log.schema_name,
             log.table_name,
             log.error,
             log.movements) for log in logs]

        df = (spark
              .createDataFrame(data, schema=self.SCHEMA)
              .withColumn("ingestion_date", current_timestamp())
              )

        df.write.format("delta") \
            .mode("append") \
            .saveAsTable(self.table_name)
