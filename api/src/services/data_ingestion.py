from databricks.sdk import WorkspaceClient
from lib.pagination import Paginator
from databricks.sdk.service.sql import StatementState, StatementParameterListItem


class DataIngestionService:
    def __init__(self, workspace_client: WorkspaceClient, warehouse_id: str):
        self.workspace_client = workspace_client
        self.warehouse_id = warehouse_id

    def get(self, page: int, page_length: int):
        return Paginator(
            workspace_client=self.workspace_client,
            warehouse_id=self.warehouse_id,
            catalog="bruno_vieira_ctr",
            schema="loading",
            table="data_ingestion",
            page_length=page_length,
            page=page,
            order="schema_name, table_name"
        ).get_result()

    def get_logs(self, page: int, page_length: int):
        return Paginator(
            workspace_client=self.workspace_client,
            warehouse_id=self.warehouse_id,
            catalog="bruno_vieira_ctr",
            schema="loading",
            table="data_ingestion_log",
            page_length=page_length,
            page=page,
            order="""
                target_catalog_name,
                target_schema_name,
                target_table_name,
                ingestion_date DESC
            """
        ).get_result()

    def update_status(self, schema_name: str, table_name: str, active: bool):
        statement = f"""
            UPDATE
                data_ingestion
            SET
                active = :active
            WHERE
                schema_name = :schema_name AND
                table_name = :table_name
        """

        response = self.workspace_client.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id,
            catalog="bruno_vieira_ctr",
            schema="loading",
            statement=statement,
            parameters=[
                StatementParameterListItem("active", "BOOLEAN", active),
                StatementParameterListItem(
                    "schema_name", "STRING", schema_name),
                StatementParameterListItem("table_name", "STRING", table_name)
            ]
        )

        if response.status.state == StatementState.FAILED:
            raise RuntimeError(response.status.error)
