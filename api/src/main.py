import os
from fastapi import FastAPI, Header, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from services.data_ingestion import DataIngestionService
from databricks.sdk.errors.platform import PermissionDenied

load_dotenv()


async def verify_token(request: Request, token: str = Header(...)):
    if not token:
        raise HTTPException(status_code=401, detail="Empty token")

    host = os.getenv("DATABRICKS_HOST")
    warehouse_id = os.getenv("WAREHOUSE_ID")

    workspace_client = WorkspaceClient(host=host, token=token)

    request.state.data_ingestion_service = DataIngestionService(
        workspace_client,
        warehouse_id
    )

app = FastAPI(
    title="AdventureWorks EL API",
    description="This API interacts with the AventureWorks data ingestion control panel in Databricks.",
    version="1.0.0",
    dependencies=[Depends(verify_token)]
)


@app.get("/", summary="Get information about tables in the EL pipeline")
def get(request: Request, page: int, page_length: int):
    service = request.state.data_ingestion_service

    return service.get(page, page_length)


@app.get("/logs/", summary="Get log history of all data movements")
def get_logs(request: Request, page: int, page_length: int):
    service = request.state.data_ingestion_service

    return service.get_logs(page, page_length)


@app.patch("/{schema_name}/{table_name}/activate/",
           summary="Activate ingestion of specified table")
def activate(request: Request, schema_name: str, table_name: str):
    service = request.state.data_ingestion_service

    service.update_status(schema_name, table_name, True)

    return "OK"


@app.patch("/{schema_name}/{table_name}/deactivate/",
           summary="Deactivate ingestion of specified table")
def deactivate(request: Request, schema_name: str, table_name: str):
    service = request.state.data_ingestion_service

    service.update_status(schema_name, table_name, False)

    return "OK"


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": repr(exc)}
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )


@app.exception_handler(PermissionDenied)
async def permission_denied_exception_handler(
        request: Request, exc: Exception):
    return JSONResponse(
        status_code=401,
        content={"detail": repr(exc)}
    )


@app.exception_handler(ValueError)
async def permission_denied_exception_handler(
        request: Request, exc: Exception):
    return JSONResponse(
        status_code=400,
        content={"detail": repr(exc)}
    )
