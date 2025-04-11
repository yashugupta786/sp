import uuid
from datetime import datetime
from app.model.document_schema import DocumentModel, SharePointInfoModel, StepModel

class SharePointIngestionService:
    """
    Ingest documents using stored metadata in run_management.
    """

    def __init__(self, db, config):
        self.db = db
        self.config = config

    def ingest_from_existing_metadata(
        self,
        sp_client,
        tenant_id: str,
        engagement_id: str,
        year: int,
        quarter: str,
        request_id: str,
        use_case_name: str,
        is_open_source_pipeline: bool
    ):
        run_management = self.db["run_management"]
        run_documents = self.db["run_documents"]
        sharepoint_logs = self.db["sharepoint_ingestion_logs"]

        run_doc = run_management.find_one({
            "tenant_id": tenant_id,
            "engagement_id": engagement_id
        })

        if not run_doc:
            raise Exception("Run not found")

        run_id = run_doc["run_id"]
        matched_meta = next(
            (m for m in run_doc.get("metadata", [])
             if m["year"] == int(year) and m["quarter"] == quarter),
            None
        )

        if not matched_meta:
            raise Exception("Metadata for given year and quarter not found")

        year_quarter_path = matched_meta.get("year_quarter_path")
        if not year_quarter_path:
            raise Exception("year_quarter_path is missing in metadata")

        folder = sp_client.get_folder(year_quarter_path)

        all_docs = []
        seen_base64s = set()
        total_new_files = 0

        for industry in matched_meta.get("sub_product_types", []):
            industry_type = industry["sub_product_type"]
            for obligor in industry["obligors"]:
                obligor_name = obligor["obligor_name"]
                doc_run_id = obligor["doc_run_id"]
                obligor_path = f"{year_quarter_path}/{industry_type}/{obligor_name}"

                try:
                    obligor_folder = sp_client.get_folder(obligor_path)
                    sp_client.ctx.load(obligor_folder.expand(["Folders", "Files"]))
                    sp_client.ctx.execute_query()

                    existing_docs = run_documents.find({"doc_run_id": doc_run_id}, {"base64": 1})
                    existing_base64s = set(doc["base64"] for doc in existing_docs)

                    files = sp_client.traverse_files(obligor_folder)

                    for file_obj in files:
                        base64_str = sp_client.get_base64_content(file_obj)
                        if not base64_str or base64_str in seen_base64s or base64_str in existing_base64s:
                            continue

                        seen_base64s.add(base64_str)

                        doc = DocumentModel(
                            doc_run_id=doc_run_id,
                            run_id=run_id,
                            tenant_id=tenant_id,
                            engagement_id=engagement_id,
                            year=int(year),
                            quarter=quarter,
                            industry_type=industry_type,
                            obligor_name=obligor_name,
                            doc_id=str(uuid.uuid4()),
                            original_filename=file_obj.properties["Name"],
                            base64=base64_str,
                            sharepoint_info=SharePointInfoModel(
                                relative_path=file_obj.properties["ServerRelativeUrl"],
                                library_name="Documents",
                                site_url=sp_client.site_url
                            ),
                            steps=[
                                StepModel(
                                    step_name="sharepoint_ingestion",
                                    result="SUCCESS",
                                    timestamp=datetime.utcnow().isoformat()
                                )
                            ]
                        )
                        all_docs.append(doc.dict())
                        total_new_files += 1
                except Exception as e:
                    print(f"[ERROR] {obligor_path} failed: {e}")
                    continue

        if all_docs:
            run_documents.insert_many(all_docs)

        sharepoint_logs.update_one(
            {"request_id": request_id},
            {"$set": {
                "status": "sharepoint_ingestion_complete",
                "document_uploaded": total_new_files > 0,
                "total_new_files": total_new_files,
                "completed_at": datetime.utcnow().isoformat()
            }}
        )

        return {"status": "success", "message": f"{total_new_files} documents uploaded."}
