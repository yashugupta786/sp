from threading import Thread

def trigger_sharepoint_ingestion(request, db):
    request_id = str(uuid.uuid4())
    sharepoint_logs = db["sharepoint_ingestion_logs"]

    sharepoint_logs.insert_one({
        "request_id": request_id,
        "site_url": request.site_url,
        "sp_folder_path": request.sp_folder_path,
        "tenant_id": request.tenant_id,
        "engagement_id": request.engagement_id,
        "status": "pending",
        "timestamp": datetime.utcnow().isoformat(),
        "doc_run_ids": []
    })

    try:
        print(f"[INFO] Starting ingestion thread for: {request_id}")
        Thread(
            target=run_ingestion,
            args=(
                request.site_url,
                request.sp_folder_path,
                request.tenant_id,
                request.engagement_id,
                request_id,
                get_config()
            ),
            daemon=True
        ).start()

    except Exception as e:
        print(f"[ERROR] Failed to start ingestion: {e}")
        sharepoint_logs.update_one(
            {"request_id": request_id},
            {"$set": {"status": "failed", "error": str(e)}}
        )
        return JSONResponse(status_code=200, content={"status": "error", "message": "Failed to start ingestion"})

    return request_id
