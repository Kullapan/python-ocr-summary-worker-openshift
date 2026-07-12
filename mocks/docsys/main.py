from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import Response
from typing import Optional

app = FastAPI(title="Mock DOCSYS API")

@app.get("/api/v1/documents/{docid}/files/{fileid}/download")
async def download_document(
    docid: str,
    fileid: str,
    authorization: Optional[str] = Header(None)
):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
    
    # Return a sample text document
    content = f"Raw document bytes for docid={docid}, fileid={fileid}.\n" \
              f"This is a dummy contract document for testing the OCR and summarization pipeline.\n" \
              f"Entity A agrees to provide consulting services to Entity B."
    
    return Response(
        content=content,
        media_type="text/plain",
        headers={
            "Content-Disposition": f'attachment; filename="document_{docid}_{fileid}.txt"',
            "Content-Type": "text/plain"
        }
    )
