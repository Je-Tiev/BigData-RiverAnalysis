from fastapi import FastAPI, UploadFile, File, HTTPException
from minio import Minio
from minio.error import S3Error
from typing import List
import uuid

app = FastAPI(title="MinIO Upload Service")

# Kết nối MinIO
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minio123456",
    secure=False
)

BUCKET_NAME = "bigdata"


@app.on_event("startup")
def startup():
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        # Tạo tên file unique để không bị trùng
        object_name = f"{uuid.uuid4()}_{file.filename}"

        minio_client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=object_name,
            data=file.file,
            length=-1,
            part_size=10 * 1024 * 1024,  # 10MB
            content_type=file.content_type
        )

        return {
            "message": "Upload thành công",
            "object_name": object_name,
            "bucket": BUCKET_NAME
        }

    except S3Error as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/upload-multiple")
async def upload_multiple_files(files: List[UploadFile] = File(...)):
    results = []

    for file in files:
        try:
            object_name = f"{uuid.uuid4()}_{file.filename}"

            minio_client.put_object(
                bucket_name=BUCKET_NAME,
                object_name=object_name,
                data=file.file,
                length=-1,
                part_size=10 * 1024 * 1024,
                content_type=file.content_type
            )

            results.append({
                "filename": file.filename,
                "object_name": object_name,
                "status": "success"
            })

        except Exception as e:
            results.append({
                "filename": file.filename,
                "status": "failed",
                "error": str(e)
            })

    return {
        "message": "Upload nhiều file hoàn tất",
        "results": results
    }
