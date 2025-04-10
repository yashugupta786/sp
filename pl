# ---------------- app/model/sharepoint_poll_request.py ----------------
from pydantic import BaseModel

class SharePointPollRequest(BaseModel):
    request_id: str


# ---------------- app/utils/response_handler.py ----------------
def success_response(message: str = "Success", data: dict = None) -> dict:
    return {
        "status": "success",
        "message": message,
        "data": data or {}
    }

def error_response(message: str = "An error occurred", error: str = "") -> dict:
    return {
        "status": "error",
        "message": message,
        "error": error
    }

def pending_response(message: str = "Processing...") -> dict:
    return {
        "status": "pending",
        "message": message
    }

def validation_error_response(message: str = "Validation error", errors: list = None) -> dict:
    return {
        "status": "validation_error",
        "message": message,
        "errors": errors or []
    }


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
    request_id = request.request_id
    sharepoint_logs = db["sharepoint_ingestion_logs"]
    run_documents = db["run_documents"]

    log_entry = await sharepoint_logs.find_one({"request_id": request_id})
    if not log_entry:
        raise HTTPException(status_code=404, detail="Request ID not found")

    status = log_entry.get("status")
    if status == "pending":
        return JSONResponse(status_code=200, content=pending_response("Ingestion is still in progress."))

    if status == "error":
        return JSONResponse(
            status_code=200,
            content=error_response("Ingestion failed", log_entry.get("error_message", "Unknown error"))
        )

    document_uploaded = log_entry.get("document_uploaded", False)
    doc_run_ids = log_entry.get("doc_run_ids", [])
    
    if not document_uploaded or not doc_run_ids:
        return JSONResponse(
            status_code=200,
            content=success_response("No new documents were ingested")
        )

    docs = await run_documents.find({"request_id": request_id}).to_list(length=None)

    if not docs:
        return JSONResponse(
            status_code=200,
            content=success_response("No new documents were ingested")
        )

    # Group by industry and obligor
    grouped_docs = {}
    for doc in docs:
        industry = doc["industry_type"]
        obligor = doc["obligor_name"]
        file_info = {
            "doc_id": doc["doc_id"],
            "original_filename": doc["original_filename"]
        }

        if industry not in grouped_docs:
            grouped_docs[industry] = {}
        if obligor not in grouped_docs[industry]:
            grouped_docs[industry][obligor] = []

        grouped_docs[industry][obligor].append(file_info)

    response = {
        "request_id": request_id,
        "run_id": docs[0]["run_id"],
        "tenant_id": docs[0]["tenant_id"],
        "engagement_id": docs[0]["engagement_id"],
        "total_files": len(docs),
        "documents": grouped_docs
    }

    return JSONResponse(status_code=200, content=success_response("Documents fetched successfully", response))


# ---------------- app/main.py ----------------
from fastapi import FastAPI
from app.routes.sharepoint import router as sharepoint_router

app = FastAPI(title="Loan Review Platform API")

# Register SharePoint Routes
app.include_router(sharepoint_router, prefix="/api/sharepoint", tags=["SharePoint Ingestion"])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
