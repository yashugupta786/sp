def fetch_excel_from_root_folder(
    self,
    folder,
    sp_client,
    run_documents,
    run_id,
    tenant_id,
    engagement_id,
    year,
    quarter,
    request_id,
    seen_base64s,
    all_docs
):
    """
    Checks and ingests Excel file (if present) directly inside year/quarter folder.
    """
    from app.model.document_schema import DocumentModel, SharePointInfoModel, StepModel
    import uuid
    from datetime import datetime

    folder_files = sp_client.traverse_files(folder)
    for file_obj in folder_files:
        file_name = file_obj.properties.get("Name", "")
        if not (file_name.endswith(".xlsx") or file_name.endswith(".xls")):
            continue  # Skip non-Excel files

        base64_str = sp_client.get_base64_content(file_obj)
        if not base64_str or base64_str in seen_base64s:
            continue

        seen_base64s.add(base64_str)

        doc = DocumentModel(
            doc_run_id=f"EXCEL_{quarter}_{year}",
            run_id=run_id,
            tenant_id=tenant_id,
            engagement_id=engagement_id,
            year=int(year),
            quarter=quarter,
            sub_product_type="EXCEL",
            obligor_name="EXCEL_FILE",
            doc_id=str(uuid.uuid4()),
            original_filename=file_name,
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
