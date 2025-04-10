# ---------------- app/model/sharepoint_poll_request.py ----------------
from pydantic import BaseModel

class SharePointPollRequest(BaseModel):
    request_id: str


# ---------------- app/utils/response_handler.py ----------------
def success_response(data: dict = None, message: str = "") -> dict:
    if data is None:
        data = {}
    return {"status": "success", "message": message, **data}

def error_response(message: str = "An error occurred") -> dict:
    return {"status": "error", "message": message}

def pending_response(message: str = "Processing") -> dict:
    return {"status": "pending", "message": message}


# ---------------- app/routes/sharepoint.py ----------------
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from app.db.mongo_client import MongoDBClient
from app.model.sharepoint_poll_request import SharePointPollRequest
from app.utils.response_handler import success_response, error_response, pending_response

router = APIRouter()

db = MongoDBClient().get_database()

@router.post("/poll-sharepoint-status")
async def poll_sharepoint_status(request: SharePointPollRequest):
    """
    Poll SharePoint ingestion status using request_id.
    If completed, returns new documents grouped by industry and obligor with doc_run_id.
    """
    request_id = request.request_id
    sharepoint_logs = db["sharepoint_ingestion_logs"]
    run_documents = db["run_documents"]

    log_entry = await sharepoint_logs.find_one({"request_id": request_id})
    if not log_entry:
        raise HTTPException(status_code=404, detail="Request ID not found")

    if log_entry["status"] == "pending":
        return JSONResponse(status_code=200, content=pending_response("Ingestion in progress"))

    if log_entry["status"] == "error":
        return JSONResponse(status_code=200, content=error_response(log_entry.get("error_message", "Error occurred")))

    doc_run_ids = log_entry.get("doc_run_ids", [])
    if not doc_run_ids or log_entry.get("document_uploaded") is False:
        return JSONResponse(status_code=200, content=success_response(message="No new documents were ingested"))

    docs = await run_documents.find({"request_id": request_id}).to_list(length=None)

    if not docs:
        return JSONResponse(status_code=200, content=success_response(message="No matching documents found for request."))

    grouped_docs = {}
    for doc in docs:
        industry = doc["industry_type"]
        obligor = doc["obligor_name"]
        doc_run_id = doc["doc_run_id"]

        key = f"{industry}|{obligor}|{doc_run_id}"
        if key not in grouped_docs:
            grouped_docs[key] = {
                "industry": industry,
                "obligor_name": obligor,
                "doc_run_id": doc_run_id,
                "files": []
            }

        grouped_docs[key]["files"].append({
            "doc_id": doc["doc_id"],
            "original_filename": doc["original_filename"]
        })

    response = {
        "request_id": request_id,
        "run_id": docs[0]["run_id"],
        "tenant_id": docs[0]["tenant_id"],
        "engagement_id": docs[0]["engagement_id"],
        "total_files": len(docs),
        "documents": list(grouped_docs.values())
    }

    return JSONResponse(status_code=200, content=success_response(response))


# ---------------- app/main.py ----------------
from fastapi import FastAPI
from app.routes.sharepoint import router as sharepoint_router

app = FastAPI(title="Loan Review Platform API")

# Register SharePoint Routes
app.include_router(sharepoint_router, prefix="/api/sharepoint", tags=["SharePoint Ingestion"])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
