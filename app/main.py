"""FastAPI 入口：推演 API + 静态页面。"""

from pathlib import Path
from typing import List

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from .simulator import SimError, run_all

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="事务交错推演", version="1.0.0")


class SimulateRequest(BaseModel):
    initial: str = Field("", description="初始键值，每行 key=value")
    txns: List[str] = Field(..., description="2~3 条事务脚本")


@app.exception_handler(SimError)
async def sim_error_handler(_request, exc: SimError):
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.get("/api/health")
async def health():
    return {"ok": True}


@app.post("/api/simulate")
async def simulate(req: SimulateRequest):
    if len(req.txns) != len([t for t in req.txns if t.strip()]):
        raise HTTPException(status_code=400, detail="事务脚本不能为空")
    try:
        return run_all(req.initial, req.txns)
    except SimError as exc:
        raise SimError(str(exc))


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")
