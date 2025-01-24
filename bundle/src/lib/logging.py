from databricks.sdk.runtime import *
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import *

class Log:
    def __init__(self, catalog_name:str, schema_name:str, table_name:str, movements:int, error:str = None):
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.error = error
        self.movements = movements

class Logger:
    def __init__(self, table_name:str):
        self.table_name = table_name

    SCHEMA = StructType([
        StructField("catalog_name", StringType(), False),
        StructField("schema_name", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("error", StringType(), True),
        StructField("movements", IntegerType(), False),
    ])

    def log(self, logs: list[Log]):
        data = [(log.catalog_name, log.schema_name, log.table_name, log.error, log.movements) for log in logs]

        df = (spark
              .createDataFrame(data, schema=self.SCHEMA)
              .withColumn("ingestion_date", current_timestamp())
        )

        df.write.format("delta") \
            .mode("append") \
            .saveAsTable(self.table_name) 