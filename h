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


def no_data_response(message: str = "No new documents found") -> dict:
    return {"status": "success", "message": message, "documents": []}


# ---------------- app/routes/sharepoint.py ----------------
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from app.db.mongo_client import MongoDBClient
from app.model.sharepoint_poll_request import SharePointPollRequest
from app.utils.response_handler import (
    success_response,
    error_response,
    pending_response,
    no_data_response,
)

router = APIRouter()

# Initialize DB connection
db = MongoDBClient().get_database()

@router.post("/poll-sharepoint-status")
async def poll_sharepoint_status(request: SharePointPollRequest):
    """
    Poll SharePoint ingestion status using request_id.
    Returns grouped new documents (if any) after ingestion is complete.
    """
    try:
        request_id = request.request_id
        sharepoint_logs = db["sharepoint_ingestion_logs"]
        run_documents = db["run_documents"]

        log_entry = await sharepoint_logs.find_one({"request_id": request_id})
        if not log_entry:
            raise HTTPException(status_code=404, detail="Request ID not found")

        if log_entry.get("status") == "pending":
            return JSONResponse(status_code=200, content=pending_response("Ingestion in progress"))

        if log_entry.get("status") == "error":
            return JSONResponse(
                status_code=200,
                content=error_response(log_entry.get("error_message", "An error occurred during ingestion"))
            )

        # Check for no uploaded documents (deduplicated run)
        if (
            not log_entry.get("doc_run_ids") or
            log_entry.get("document_uploaded") is False or
            log_entry.get("file_count", 0) == 0
        ):
            return JSONResponse(
                status_code=200,
                content=no_data_response("No new documents were ingested")
            )

        # Fetch only new documents for this request
        docs = await run_documents.find({"request_id": request_id}).to_list(length=None)

        if not docs:
            return JSONResponse(
                status_code=200,
                content=no_data_response("No matching documents found for request.")
            )

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

    except Exception as e:
        return JSONResponse(status_code=500, content=error_response(str(e)))


# ---------------- app/main.py ----------------
from fastapi import FastAPI
from app.routes.sharepoint import router as sharepoint_router

app = FastAPI(title="Loan Review Platform API")

# Register SharePoint Routes
app.include_router(sharepoint_router, prefix="/api/sharepoint", tags=["SharePoint Ingestion"])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
