from fastapi import FastAPI, Header, HTTPException, UploadFile, File, Form
from typing import Optional
from pydantic import BaseModel

app = FastAPI(title="Mock Secure GPT API")

class ChatMessage(BaseModel):
    role: str
    content: str

class ChatCompletionRequest(BaseModel):
    model: str
    messages: list[ChatMessage]

@app.post("/api/v1/ocr")
async def perform_ocr(
    model: str = Form(...),
    file: UploadFile = File(...),
    authorization: Optional[str] = Header(None)
):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
    
    content_bytes = await file.read()
    content_str = content_bytes.decode("utf-8", errors="ignore")
    
    # Prepend dynamic simulated OCR tag for verification
    ocr_result = f"=== OCR START: {file.filename} ===\n{content_str}\n=== OCR END ==="
    
    return {
        "text": ocr_result
    }

@app.post("/api/v1/chat/completions")
async def chat_completions(
    request: ChatCompletionRequest,
    authorization: Optional[str] = Header(None)
):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
    
    user_content = ""
    for msg in request.messages:
        if msg.role == "user":
            user_content = msg.content
            break
            
    summary_content = (
        f"=== SUMMARY REPORT ===\n"
        f"Processed via Model: {request.model}\n\n"
        f"Key Extracted Details:\n"
        f"- Original Text Length: {len(user_content)} characters\n"
        f"- Extracted Keywords: Consulting, Entity A, Entity B\n\n"
        f"Summary:\n"
        f"The document describes consulting services provided by Entity A to Entity B.\n"
        f"=== END OF REPORT ==="
    )
    
    return {
        "choices": [
            {
                "message": {
                    "role": "assistant",
                    "content": summary_content
                }
            }
        ]
    }
