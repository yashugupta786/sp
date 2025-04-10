# ---------------- app/services/sharepoint_trigger.py ----------------
import uuid
import yaml
import asyncio
from datetime import datetime
from fastapi.concurrency import run_in_threadpool

from app.db.mongo_client import MongoDBClient
from app.sharepoint.client import SharePointClient
from app.sharepoint.ingestion_service import SharePointIngestionService

def get_config():
    """Load application configuration."""
    with open("app/config/settings.yaml", "r") as f:
        return yaml.safe_load(f)

async def trigger_sharepoint_ingestion(request, db, background_tasks):
    """
    Start the SharePoint ingestion in a background thread and return a request ID.
    """
    config = get_config()
    sharepoint_logs = db["sharepoint_ingestion_logs"]

    request_id = str(uuid.uuid4())
    await sharepoint_logs.insert_one({
        "request_id": request_id,
        "site_url": request.site_url,
        "sp_folder_path": request.sp_folder_path,
        "tenant_id": request.tenant_id,
        "engagement_id": request.engagement_id,
        "status": "pending",
        "timestamp": datetime.utcnow().isoformat(),
        "doc_run_ids": []
    })

    # Trigger as asyncio task
    asyncio.create_task(
        run_ingestion(
            request.site_url,
            request.sp_folder_path,
            request.tenant_id,
            request.engagement_id,
            request_id,
            config
        )
    )

    return request_id


async def run_ingestion(site_url, sp_folder_path, tenant_id, engagement_id, request_id, config):
    """
    This runs the full ingestion process for SharePoint in background.
    Updates sharepoint_ingestion_logs status and ingests into run_management and run_documents.
    """
    print(f"[INFO] Ingestion triggered for request_id: {request_id}")
    db = MongoDBClient().get_database()
    log_collection = db["sharepoint_ingestion_logs"]

    try:
        await run_in_threadpool(_process_blocking_ingestion, site_url, sp_folder_path, tenant_id, engagement_id, request_id, config)
    except Exception as e:
        await log_collection.update_one(
            {"request_id": request_id},
            {"$set": {"status": "failed", "error": str(e)}}
        )


def _process_blocking_ingestion(site_url, sp_folder_path, tenant_id, engagement_id, request_id, config):
    db = MongoDBClient().get_database()
    log_collection = db["sharepoint_ingestion_logs"]

    try:
        sp_client = SharePointClient(site_url, config["client_id"], config["client_secret"])
        folder = sp_client.get_folder(sp_folder_path)

        folder_name = sp_folder_path.strip("/").split("/")[-1]
        year, quarter = sp_client.extract_year_quarter(folder_name)
        if not year or not quarter:
            asyncio.run(log_collection.update_one(
                {"request_id": request_id},
                {"$set": {"status": "failed", "error": "Invalid folder name"}}
            ))
            return

        ingestion_service = SharePointIngestionService(db, config)
        result = ingestion_service.ingest(folder, sp_client, tenant_id, engagement_id, year, quarter, request_id)

        asyncio.run(log_collection.update_one(
            {"request_id": request_id},
            {"$set": {"status": result["status"], "message": result["message"]}}
        ))

    except Exception as e:
        asyncio.run(log_collection.update_one(
            {"request_id": request_id},
            {"$set": {"status": "failed", "error": str(e)}}
        ))
