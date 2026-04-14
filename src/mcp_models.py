from typing import Any

from pydantic import BaseModel, Field


class ExecuteReadonlySqlRequest(BaseModel):
    sql: str = Field(..., min_length=1)
    database: str | None = None


class ExplainReasoningRequest(BaseModel):
    question: str = Field(..., min_length=1)
    chosen_tables: list[str]
    sql: str = Field(..., min_length=1)


class PreviewTableRequest(BaseModel):
    table_name: str = Field(..., min_length=1)
    schema_name: str = Field(default='dbo', min_length=1)
    database: str | None = None


class DownloadResultRequest(BaseModel):
    sql: str = Field(..., min_length=1)
    file_name: str | None = None
    download_mode: str = Field(default='link', min_length=1)
    database: str | None = None


class BuildChartRequest(BaseModel):
    sql: str = Field(..., min_length=1)
    chart_type: str = Field(default='bar', min_length=1)
    x_field: str | None = None
    y_field: str | None = None
    series_field: str | None = None
    title: str | None = None
    database: str | None = None


class BuildDashboardRequest(BaseModel):
    title: str | None = None
    widgets: list[dict[str, Any]] = Field(...)
    database: str | None = None
