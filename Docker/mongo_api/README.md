# mongo_api

FastAPI service to query river/water-quality records stored in MongoDB.

Quick start (from `Docker/` folder):

1. Start services:

```powershell
docker-compose up -d mongodb mongo-express mongo_api
```

2. Load sample CSV data into MongoDB (mounts project root into the container):

```powershell
docker-compose run --rm mongo_api python load_data.py
```

3. API endpoints:
- `GET /health` — health check
- `GET /records?start=YYYY-MM-DD&end=YYYY-MM-DD&site=NAME&limit=100` — query records

Notes:
- The docker-compose mounts the project root as read-only to `/app/data` so the loader can access `kafka/sorted_water_quality.csv`.
- MongoDB root user is `root` / `password123` in this compose setup. Update as needed for production.
