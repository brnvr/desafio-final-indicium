from databricks.sdk import WorkspaceClient
import math
from lib.databricks_sdk_utils import get_results_dict
from databricks.sdk.service.sql import StatementState


class PaginationResult:
    def __init__(self,
                 page: int,
                 page_length: int,
                 total_results: int,
                 data: list):

        self.page = page
        self.page_length = page_length
        self.total_pages = 1 if page_length == 0 else math.floor(
            total_results / page_length)
        self.total_results = total_results
        self.data = data


class Paginator:
    def __init__(self,
                 workspace_client: WorkspaceClient,
                 table: str,
                 warehouse_id: str,
                 catalog: str,
                 schema: str,
                 page_length: int,
                 page: int,
                 order: int = 0,
                 select: str = "*"):

        self.workspace_client = workspace_client
        self.table = table
        self.select = select
        self.order = order
        self.page_length = page_length
        self.page = page
        self.catalog = catalog
        self.schema = schema
        self.warehouse_id = warehouse_id

    def __get_total_results__(self):
        statement = f"""
            SELECT COUNT(*)
            FROM {self.table}
        """

        response = self.workspace_client.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id,
            catalog=self.catalog,
            schema=self.schema,
            statement=statement
        )

        if response.status.state == StatementState.FAILED:
            raise RuntimeError(response.status.error)

        return int(response.result.data_array[0][0])

    def get_result(self):
        if self.page < 0:
            raise ValueError("page must be greater than or equal to 0.")

        if self.page_length < 0:
            raise ValueError("page_length must be greater than or equal to 0.")

        statement = f"""
            SELECT {self.select}
            FROM {self.table}
            ORDER BY {self.order}
        """

        if self.page_length > 0:
            statement += f"""
                LIMIT {self.page_length}
                OFFSET {self.page * self.page_length}
            """

        response = self.workspace_client.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id,
            catalog=self.catalog,
            schema=self.schema,
            statement=statement
        )

        if response.status.state == StatementState.FAILED:
            raise RuntimeError(response.status.error)

        total_results = self.__get_total_results__()

        if total_results > self.page * self.page_length:
            data = get_results_dict(response)
        else:
            data = []

        return PaginationResult(
            page=self.page,
            page_length=self.page_length,
            data=data,
            total_results=total_results
        )
