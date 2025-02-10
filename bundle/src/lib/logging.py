from databricks.sdk.runtime import spark
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructField, StringType, IntegerType, StructType


class Log:
    """
    Log information from data movements.

    Attributes:
        source_catalog_name (str): The Name of the Unity Catalog catalog where
            the source table is located.
        source_schema_name (str): The source schema.
        source_table_name (str): The source table.
        target_catalog_name (str): The Name of the Unity Catalog catalog where
            the target table is located.
        target_schema_name (str): The target schema.
        target_table_name (str): The target table.
        movements (int): The number of movements, i.e., rows ingested by the
            source table.
        error (str): An error message, if the ingestion failed, or None, if it
        was successful.
    """

    def __init__(
        self,
        target_catalog_name: str,
        target_schema_name: str,
        target_table_name: str,
        source_catalog_name: str,
        source_schema_name: str,
        source_table_name: str,
        movements: int = None,
        error: str = None
    ):
        self.source_catalog_name = source_catalog_name
        self.source_schema_name = source_schema_name
        self.source_table_name = source_table_name
        self.target_catalog_name = target_catalog_name
        self.target_schema_name = target_schema_name
        self.target_table_name = target_table_name
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
        StructField("source_catalog_name", StringType(), True),
        StructField("source_schema_name", StringType(), False),
        StructField("source_table_name", StringType(), False),
        StructField("target_catalog_name", StringType(), False),
        StructField("target_schema_name", StringType(), False),
        StructField("target_table_name", StringType(), False),
        StructField("error", StringType(), True),
        StructField("movements", IntegerType(), True),
    ])

    def log(self, logs: list[Log]):
        """
        Logs data movements.

        Args:
            logs (list[Log]): A list of data movement logs.
        """
        data = [
            (log.source_catalog_name,
             log.source_schema_name,
             log.source_table_name,
             log.target_catalog_name,
             log.target_schema_name,
             log.target_table_name,
             log.error,
             log.movements) for log in logs]

        df = (spark
              .createDataFrame(data, schema=self.SCHEMA)
              .withColumn("ingestion_date", current_timestamp())
              )

        df.write.format("delta") \
            .mode("append") \
            .saveAsTable(self.table_name)
