from pathlib import Path

file_path = Path("server/python/routers/parquet.py")
content = file_path.read_text()

content = content.replace(
    "from fastapi import APIRouter, HTTPException, Query, UploadFile, File",
    "from fastapi import APIRouter, HTTPException, Query, UploadFile, File\nfrom fastapi.concurrency import run_in_threadpool"
)

content = content.replace(
    """    with open(file_path, "wb") as f:
        f.write(content)""",
    """    await run_in_threadpool(file_path.write_bytes, content)"""
)

file_path.write_text(content)
