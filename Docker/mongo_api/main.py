from fastapi import FastAPI, Query, HTTPException
from typing import Optional
from datetime import datetime
import os
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:password123@mongodb:27017/?authSource=admin")
client = MongoClient(MONGO_URI)
db = client['river_data']
col = db['measurements']

app = FastAPI(title="MongoAPI")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/records")
def get_records(start: Optional[str] = Query(None), end: Optional[str] = Query(None), site: Optional[str] = Query(None), limit: int = 100):
    query = {}
    if start or end:
        time_q = {}
        if start:
            try:
                time_q["$gte"] = datetime.fromisoformat(start)
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid start datetime. Use ISO format.")
        if end:
            try:
                time_q["$lte"] = datetime.fromisoformat(end)
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid end datetime. Use ISO format.")
        query["timestamp"] = time_q
    if site:
        query["site"] = {"$regex": site, "$options": "i"}

    cursor = col.find(query).limit(limit)
    results = []
    for doc in cursor:
        doc["_id"] = str(doc["_id"])
        if isinstance(doc.get("timestamp"), datetime):
            doc["timestamp"] = doc["timestamp"].isoformat()
        results.append(doc)
    return {"count": len(results), "results": results}
