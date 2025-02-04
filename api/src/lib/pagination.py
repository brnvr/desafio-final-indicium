from typing import Union
from databricks.sdk import WorkspaceClient
import math
from lib.databricks_sdk_utils import get_results_dict
from databricks.sdk.service.sql import StatementState


class PaginationResult:
    """
    Encapsulates the results of a paginated search.

    Attributes:
        page (int): The current page being displayed.
        page_length (int): The number of entries per page.
        total_pages (int): The total number of pages returned.
        total_results (int): The total number of entries returned.
        data (list): The list of entries in the current page.
    """
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
    """
    Queries data from Databricks in paginated format.

    Attributes:
        workspace_client (WorkspaceClient): The Databricks workspace client.
        warehouse_id (str): The id of the SQL warehouse to execute the query.
        table (str): The name of the table to be queried.
        schema (str): The name of the schema where the table is located.
        catalog (str): The name of the catalog where the schema is located.
        page_length (int): The number of entries per page.
        page (int): The current page to be displayed.
        order (list[Union[int, str]]): A list of columns to order the query. If
            unspecified, the first column will be used.
        select (list[Union[int, str]]): A list of columns to be selected, or
            None, for all columns.
    """
    def __init__(self,
                 workspace_client: WorkspaceClient,
                 table: str,
                 warehouse_id: str,
                 catalog: str,
                 schema: str,
                 page_length: int,
                 page: int,
                 order: list[Union[int, str]] = [0],
                 select: list[Union[int, str]] = None):

        self.workspace_client = workspace_client
        self.table = table
        self.select = select
        self.order = order
        self.page_length = page_length
        self.page = page
        self.catalog = catalog
        self.schema = schema
        self.warehouse_id = warehouse_id
        self.__execution_tries__ = 0

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
        """
        Returns:
            PaginationResult: The result of the query.
        """
        self.__execution_tries__ = 0

        if self.page < 0:
            raise ValueError("page must be greater than or equal to 0.")

        if self.page_length < 0:
            raise ValueError("page_length must be greater than or equal to 0.")

        statement = f"""
            SELECT {"*" if self.select is None else ",".join(self.select)}
            FROM {self.table}
            ORDER BY {",".join(self.order)}
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

        if response.status.state != StatementState.SUCCEEDED:
            self.__execution_tries__ += 1

            if self.__execution_tries__ > 5:
                raise RuntimeError("Execution could not be completed.")

            return self.get_result()

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
