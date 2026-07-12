from fastapi import FastAPI, Header, HTTPException, UploadFile, File
from typing import Optional

app = FastAPI(title="Mock DOCSYSARC API")

@app.post("/api/v1/documents/{docid}/files/{fileid}/archive")
async def archive_document(
    docid: str,
    fileid: str,
    file: UploadFile = File(...),
    authorization: Optional[str] = Header(None)
):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
        
    content = await file.read()
    
    return {
        "status": "SUCCESS",
        "message": f"Successfully archived file {file.filename} ({len(content)} bytes) for document {docid}/{fileid}",
        "archive_id": f"mock_archive_id_{docid}_{fileid}"
    }
