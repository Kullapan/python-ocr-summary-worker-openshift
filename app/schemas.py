"""Input message validation schemas for the OCR Summary Worker."""

from __future__ import annotations

from pydantic import BaseModel, Field


class JobStartMessage(BaseModel):
    """Pydantic model representing the input payload to start a document processing job."""

    job_id: str = Field(
        ...,
        min_length=1,
        description="Client-generated unique correlation/job ID (any string format)",
    )
    docid: str = Field(
        ...,
        min_length=1,
        description="Document identifier in DOCSYS",
    )
    fileid: str = Field(
        ...,
        min_length=1,
        description="File identifier within the document",
    )
