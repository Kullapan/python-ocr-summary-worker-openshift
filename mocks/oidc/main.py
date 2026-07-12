from fastapi import FastAPI, Form
from typing import Optional
import time

app = FastAPI(title="Mock OIDC Provider")

@app.post("/token")
@app.post("/auth/realms/company/protocol/openid-connect/token")
async def token(
    grant_type: str = Form(...),
    client_id: str = Form(...),
    client_secret: str = Form(...),
    scope: Optional[str] = Form(None),
    audience: Optional[str] = Form(None),
):
    # Simulated access token valid for 3600 seconds
    return {
        "access_token": f"mock_token_{client_id}_{int(time.time())}",
        "expires_in": 3600,
        "token_type": "Bearer",
        "scope": scope or ""
    }
