"""
CONVERTENAHORA — PDF → DOCX conversion service (server-side).

Same job-queue pattern as Convertaqui (FastAPI):
  POST /api/jobs
  PUT  /api/jobs/{id}/file
  GET  /api/jobs/{id}
  GET  /api/jobs/{id}/download

Engine: pdf2docx (best open-source layout/table reconstruction).
Optional LibreOffice fallback when SOFFICE_PATH is set.
"""

from __future__ import annotations

import asyncio
import os
import re
import secrets
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

MAX_BYTES = int(os.environ.get("MAX_UPLOAD_MB", "20")) * 1024 * 1024
JOB_TTL_SEC = int(os.environ.get("JOB_TTL_SEC", "120"))
API_KEY = (os.environ.get("CONVERTER_API_KEY") or "").strip()
SOFFICE = (os.environ.get("SOFFICE_PATH") or shutil.which("soffice") or "").strip()
ALLOWED_ORIGINS = [
    o.strip()
    for o in (os.environ.get("CORS_ORIGINS") or "*").split(",")
    if o.strip()
]

app = FastAPI(title="CONVERTENAHORA Converter", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS if ALLOWED_ORIGINS != ["*"] else ["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


class JobStatus(str, Enum):
    queued = "queued"
    processing = "processing"
    ready = "ready"
    failed = "failed"
    expired = "expired"
    cancelled = "cancelled"
    consumed = "consumed"


@dataclass
class Job:
    id: str
    operation: str
    filename: str
    status: JobStatus = JobStatus.queued
    created: float = field(default_factory=time.time)
    error: str = ""
    position: int = 1
    pdf_path: Optional[Path] = None
    docx_path: Optional[Path] = None
    engine: str = ""


JOBS: dict[str, Job] = {}
QUEUE: asyncio.Queue[str] = asyncio.Queue()
WORKER_STARTED = False


class CreateJobBody(BaseModel):
    operation: str = Field(default="pdf-docx")
    filename: str = Field(default="document.pdf")
    turnstile_token: str = Field(default="")


def require_key(x_api_key: Optional[str]) -> None:
    if not API_KEY:
        return
    if (x_api_key or "").strip() != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


def purge_expired() -> None:
    now = time.time()
    dead = []
    for jid, job in JOBS.items():
        if now - job.created > JOB_TTL_SEC + 30:
            dead.append(jid)
        elif job.status in (JobStatus.ready, JobStatus.failed) and now - job.created > JOB_TTL_SEC:
            job.status = JobStatus.expired
            cleanup_files(job)
    for jid in dead:
        cleanup_files(JOBS.pop(jid, None))


def cleanup_files(job: Optional[Job]) -> None:
    if not job:
        return
    for p in (job.pdf_path, job.docx_path):
        try:
            if p and p.exists():
                p.unlink()
        except OSError:
            pass
    if job.pdf_path:
        try:
            parent = job.pdf_path.parent
            if parent.exists() and parent.name.startswith("cq-"):
                shutil.rmtree(parent, ignore_errors=True)
        except OSError:
            pass


def safe_stem(name: str) -> str:
    base = Path(name or "document").stem
    base = re.sub(r"[^\w\-]+", "_", base, flags=re.UNICODE).strip("_")
    return (base or "document")[:80]


def convert_with_pdf2docx(pdf: Path, docx: Path) -> None:
    from pdf2docx import Converter

    cv = Converter(str(pdf))
    try:
        cv.convert(str(docx))
    finally:
        cv.close()
    if not docx.exists() or docx.stat().st_size < 64:
        raise RuntimeError("pdf2docx produced an empty document")


def convert_with_libreoffice(pdf: Path, out_dir: Path) -> Path:
    if not SOFFICE:
        raise RuntimeError("LibreOffice not available")
    cmd = [
        SOFFICE,
        "--headless",
        "--norestore",
        "--nolockcheck",
        "--infilter=writer_pdf_import",
        "--convert-to",
        "docx",
        "--outdir",
        str(out_dir),
        str(pdf),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "LibreOffice failed")
    expected = out_dir / f"{pdf.stem}.docx"
    if not expected.exists():
        matches = list(out_dir.glob("*.docx"))
        if not matches:
            raise RuntimeError("LibreOffice did not write a DOCX")
        expected = matches[0]
    return expected


def convert_pdf(job: Job) -> None:
    assert job.pdf_path and job.pdf_path.exists()
    work = job.pdf_path.parent
    target = work / f"{safe_stem(job.filename)}.docx"

    errors: list[str] = []
    try:
        convert_with_pdf2docx(job.pdf_path, target)
        job.docx_path = target
        job.engine = "pdf2docx"
        return
    except Exception as exc:  # noqa: BLE001
        errors.append(f"pdf2docx: {exc}")

    if SOFFICE:
        try:
            lo = convert_with_libreoffice(job.pdf_path, work)
            if lo != target:
                shutil.move(str(lo), str(target))
            job.docx_path = target
            job.engine = "libreoffice"
            return
        except Exception as exc:  # noqa: BLE001
            errors.append(f"libreoffice: {exc}")

    raise RuntimeError(" | ".join(errors) or "conversion failed")


async def worker_loop() -> None:
    while True:
        jid = await QUEUE.get()
        job = JOBS.get(jid)
        if not job or job.status == JobStatus.cancelled:
            QUEUE.task_done()
            continue
        job.status = JobStatus.processing
        job.position = 0
        try:
            await asyncio.to_thread(convert_pdf, job)
            if job.status != JobStatus.cancelled:
                job.status = JobStatus.ready
        except Exception as exc:  # noqa: BLE001
            job.status = JobStatus.failed
            job.error = str(exc)[:400]
            cleanup_files(job)
        finally:
            QUEUE.task_done()


@app.on_event("startup")
async def on_startup() -> None:
    global WORKER_STARTED
    if not WORKER_STARTED:
        WORKER_STARTED = True
        asyncio.create_task(worker_loop())


@app.get("/health")
def health() -> dict:
    return {
        "ok": True,
        "engine": "pdf2docx",
        "libreoffice": bool(SOFFICE),
        "max_mb": MAX_BYTES // (1024 * 1024),
        "ttl_sec": JOB_TTL_SEC,
    }


@app.post("/api/jobs")
async def create_job(
    body: CreateJobBody,
    x_api_key: Optional[str] = Header(default=None),
) -> dict:
    require_key(x_api_key)
    purge_expired()
    op = (body.operation or "pdf-docx").strip().lower()
    if op not in {"pdf-docx", "pdf-doc", "pdf-to-word"}:
        raise HTTPException(status_code=400, detail="Unsupported operation")
    jid = secrets.token_urlsafe(24)
    job = Job(id=jid, operation="pdf-docx", filename=body.filename or "document.pdf")
    JOBS[jid] = job
    return {"id": jid, "status": job.status.value, "expires_in": JOB_TTL_SEC}


@app.put("/api/jobs/{job_id}/file")
async def upload_file(
    job_id: str,
    request: Request,
    x_api_key: Optional[str] = Header(default=None),
) -> dict:
    require_key(x_api_key)
    purge_expired()
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status not in (JobStatus.queued,):
        raise HTTPException(status_code=409, detail="Job not accepting file")

    data = await request.body()
    if not data:
        raise HTTPException(status_code=400, detail="Empty body")
    if len(data) > MAX_BYTES:
        raise HTTPException(status_code=413, detail="File too large")
    if data[:4] != b"%PDF":
        raise HTTPException(status_code=415, detail="Not a PDF")

    work = Path(tempfile.mkdtemp(prefix="cq-"))
    pdf_path = work / f"{safe_stem(job.filename)}.pdf"
    pdf_path.write_bytes(data)
    job.pdf_path = pdf_path
    job.position = max(1, QUEUE.qsize() + 1)
    await QUEUE.put(job.id)
    return {"id": job.id, "status": JobStatus.queued.value, "position": job.position}


@app.get("/api/jobs/{job_id}")
def job_status(
    job_id: str,
    x_api_key: Optional[str] = Header(default=None),
) -> dict:
    require_key(x_api_key)
    purge_expired()
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    expires_in = max(0, int(JOB_TTL_SEC - (time.time() - job.created)))
    if expires_in <= 0 and job.status in (JobStatus.ready, JobStatus.queued, JobStatus.processing):
        job.status = JobStatus.expired
        cleanup_files(job)
    return {
        "id": job.id,
        "status": job.status.value,
        "position": job.position if job.status == JobStatus.queued else 0,
        "expires_in": expires_in,
        "engine": job.engine,
        "error": job.error if job.status == JobStatus.failed else "",
    }


@app.get("/api/jobs/{job_id}/download")
def download(
    job_id: str,
    x_api_key: Optional[str] = Header(default=None),
) -> Response:
    require_key(x_api_key)
    purge_expired()
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status not in (JobStatus.ready, JobStatus.consumed):
        raise HTTPException(status_code=409, detail="Not ready")
    if not job.docx_path or not job.docx_path.exists():
        raise HTTPException(status_code=410, detail="File gone")
    name = f"{safe_stem(job.filename)}.docx"
    job.status = JobStatus.consumed
    return FileResponse(
        path=str(job.docx_path),
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        filename=name,
    )


@app.post("/api/jobs/{job_id}/cancel")
def cancel_job(
    job_id: str,
    x_api_key: Optional[str] = Header(default=None),
) -> dict:
    require_key(x_api_key)
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    job.status = JobStatus.cancelled
    cleanup_files(job)
    return {"id": job.id, "status": "cancelled"}


@app.post("/api/convert")
async def convert_direct(
    request: Request,
    x_api_key: Optional[str] = Header(default=None),
    x_filename: Optional[str] = Header(default="document.pdf"),
) -> Response:
    """One-shot convert (used by Cloudflare Pages Function proxy)."""
    require_key(x_api_key)
    data = await request.body()
    if not data or data[:4] != b"%PDF":
        raise HTTPException(status_code=415, detail="Not a PDF")
    if len(data) > MAX_BYTES:
        raise HTTPException(status_code=413, detail="File too large")

    work = Path(tempfile.mkdtemp(prefix="cq-"))
    try:
        pdf = work / "input.pdf"
        pdf.write_bytes(data)
        job = Job(id="direct", operation="pdf-docx", filename=x_filename or "document.pdf", pdf_path=pdf)
        await asyncio.to_thread(convert_pdf, job)
        assert job.docx_path and job.docx_path.exists()
        content = job.docx_path.read_bytes()
        name = f"{safe_stem(job.filename)}.docx"
        return Response(
            content=content,
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers={
                "Content-Disposition": f'attachment; filename="{name}"',
                "X-Converter-Engine": job.engine or "pdf2docx",
            },
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)[:300]) from exc
    finally:
        shutil.rmtree(work, ignore_errors=True)
