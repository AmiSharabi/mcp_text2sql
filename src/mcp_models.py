from pydantic import BaseModel, Field


class ExecuteReadonlySqlRequest(BaseModel):
    sql: str = Field(..., min_length=1)


class ExplainReasoningRequest(BaseModel):
    question: str = Field(..., min_length=1)
    chosen_tables: list[str]
    sql: str = Field(..., min_length=1)


class PreviewTableRequest(BaseModel):
    table_name: str = Field(..., min_length=1)
    schema_name: str = Field(default='dbo', min_length=1)


class DownloadResultRequest(BaseModel):
    sql: str = Field(..., min_length=1)
    file_name: str | None = None
    download_mode: str = Field(default='link', min_length=1)
