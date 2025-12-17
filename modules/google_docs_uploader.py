"""
Google Docs Uploader Module
Automatically uploads market breadth data to Google Docs as a table

Strategy: Find doc by name in folder → Delete if exists → Create new doc → Insert table
This ensures a stable workflow without needing to update doc ID each time.
"""
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import pandas as pd
import streamlit as st


def get_services():
    """
    Create Google Docs and Drive API services using service account credentials

    Returns:
        tuple: (docs_service, drive_service) or (None, None) on error
    """
    try:
        # Get credentials from Streamlit secrets
        credentials_dict = dict(st.secrets["gcp_service_account"])

        # Define the scope
        scope = [
            'https://www.googleapis.com/auth/documents',
            'https://www.googleapis.com/auth/drive'
        ]

        # Create credentials
        credentials = Credentials.from_service_account_info(
            credentials_dict,
            scopes=scope
        )

        # Build services
        docs_service = build('docs', 'v1', credentials=credentials)
        drive_service = build('drive', 'v3', credentials=credentials)
        
        return docs_service, drive_service

    except Exception as e:
        st.warning(f"Cannot connect to Google APIs: {str(e)}")
        return None, None


def find_doc_by_name(drive_service, doc_name, folder_id):
    """
    Find a Google Doc by name in a specific folder
    
    Args:
        drive_service: Google Drive API service
        doc_name: Document name to search for
        folder_id: Folder ID to search in
    
    Returns:
        str: Document ID if found, None otherwise
    """
    try:
        query = f"name='{doc_name}' and '{folder_id}' in parents and mimeType='application/vnd.google-apps.document' and trashed=false"
        results = drive_service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)'
        ).execute()
        
        files = results.get('files', [])
        if files:
            return files[0]['id']
        return None
        
    except Exception as e:
        st.warning(f"Error searching for doc: {str(e)}")
        return None


def delete_doc_by_id(drive_service, doc_id):
    """
    Delete a Google Doc by its ID
    
    Args:
        drive_service: Google Drive API service
        doc_id: Document ID to delete
    
    Returns:
        bool: True if successful or doc doesn't exist
    """
    try:
        drive_service.files().delete(fileId=doc_id).execute()
        return True
    except Exception as e:
        # If file not found, that's OK
        if 'notFound' in str(e) or '404' in str(e):
            return True
        st.warning(f"Error deleting doc: {str(e)}")
        return False


def create_new_doc(docs_service, drive_service, title, folder_id):
    """
    Create a new Google Doc directly in a specific folder using Drive API
    
    Args:
        docs_service: Google Docs API service (not used but kept for consistency)
        drive_service: Google Drive API service
        title: Document title
        folder_id: Folder ID to create doc in
    
    Returns:
        str: New document ID or None on error
    """
    try:
        # Create doc directly in folder using Drive API
        file_metadata = {
            'name': title,
            'mimeType': 'application/vnd.google-apps.document',
            'parents': [folder_id]
        }
        
        file = drive_service.files().create(
            body=file_metadata,
            fields='id'
        ).execute()
        
        new_doc_id = file.get('id')
        return new_doc_id
        
    except Exception as e:
        st.error(f"Error creating doc: {str(e)}")
        return None


def insert_table_with_data(docs_service, doc_id, df):
    """
    Insert a table with data into a Google Doc
    
    Args:
        docs_service: Google Docs API service
        doc_id: Document ID
        df: DataFrame to insert
    
    Returns:
        bool: True if successful
    """
    try:
        rows = len(df) + 1  # +1 for header
        cols = len(df.columns)
        
        # Insert table
        insert_table_request = {
            'insertTable': {
                'rows': rows,
                'columns': cols,
                'location': {'index': 1}
            }
        }
        
        docs_service.documents().batchUpdate(
            documentId=doc_id,
            body={'requests': [insert_table_request]}
        ).execute()
        
        # Get the document to find table cell indices
        doc = docs_service.documents().get(documentId=doc_id).execute()
        content = doc.get('body', {}).get('content', [])
        
        # Find the table element
        table = None
        for element in content:
            if 'table' in element:
                table = element['table']
                break
        
        if table is None:
            st.warning("Failed to find inserted table")
            return False
        
        # Prepare all data (header + rows)
        all_data = [df.columns.tolist()] + df.fillna('').values.tolist()
        
        # Collect all cell insertions
        cell_insertions = []
        
        for row_idx, table_row in enumerate(table.get('tableRows', [])):
            for col_idx, cell in enumerate(table_row.get('tableCells', [])):
                cell_content = cell.get('content', [])
                if cell_content:
                    para = cell_content[0]
                    if 'paragraph' in para:
                        start_index = para.get('startIndex', 0)
                        
                        if row_idx < len(all_data) and col_idx < len(all_data[row_idx]):
                            text = str(all_data[row_idx][col_idx])
                            if text:
                                cell_insertions.append({
                                    'index': start_index,
                                    'text': text
                                })
        
        # Sort by index descending (insert from end to start)
        cell_insertions.sort(key=lambda x: x['index'], reverse=True)
        
        # Create insert text requests
        text_requests = []
        for insertion in cell_insertions:
            text_requests.append({
                'insertText': {
                    'location': {'index': insertion['index']},
                    'text': insertion['text']
                }
            })
        
        # Execute text insertions in batches
        batch_size = 100
        for i in range(0, len(text_requests), batch_size):
            batch = text_requests[i:i + batch_size]
            docs_service.documents().batchUpdate(
                documentId=doc_id,
                body={'requests': batch}
            ).execute()
        
        # Format header row (bold)
        doc = docs_service.documents().get(documentId=doc_id).execute()
        content = doc.get('body', {}).get('content', [])
        
        for element in content:
            if 'table' in element:
                table = element['table']
                first_row = table.get('tableRows', [])[0] if table.get('tableRows') else None
                if first_row:
                    format_requests = []
                    for cell in first_row.get('tableCells', []):
                        cell_content = cell.get('content', [])
                        if cell_content:
                            para = cell_content[0]
                            if 'paragraph' in para:
                                start_idx = para.get('startIndex', 0)
                                end_idx = para.get('endIndex', start_idx + 1)
                                if end_idx > start_idx + 1:
                                    format_requests.append({
                                        'updateTextStyle': {
                                            'range': {
                                                'startIndex': start_idx,
                                                'endIndex': end_idx - 1
                                            },
                                            'textStyle': {'bold': True},
                                            'fields': 'bold'
                                        }
                                    })
                    
                    if format_requests:
                        try:
                            docs_service.documents().batchUpdate(
                                documentId=doc_id,
                                body={'requests': format_requests}
                            ).execute()
                        except:
                            pass  # Silent fail for formatting
                break
        
        return True
        
    except Exception as e:
        st.error(f"Error inserting table: {str(e)}")
        return False


def upload_to_google_doc(df, folder_id, doc_name="Market Breadth Data"):
    """
    Upload dataframe to Google Doc as a table
    
    Strategy: Find existing doc by name → Delete it → Create new doc → Insert table
    
    Args:
        df: DataFrame to upload
        folder_id: Google Drive folder ID where doc will be created
        doc_name: Name for the document
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get API services
        docs_service, drive_service = get_services()
        if docs_service is None or drive_service is None:
            return False
        
        # Step 1: Find existing doc by name in folder
        existing_doc_id = find_doc_by_name(drive_service, doc_name, folder_id)
        
        # Step 2: Delete old doc if exists
        if existing_doc_id:
            if not delete_doc_by_id(drive_service, existing_doc_id):
                st.warning("Could not delete old document")
                # Continue anyway - will create with unique name
        
        # Step 3: Create new doc with same name
        new_doc_id = create_new_doc(docs_service, drive_service, doc_name, folder_id)
        if new_doc_id is None:
            return False
        
        # Step 4: Insert table with data
        if not insert_table_with_data(docs_service, new_doc_id, df):
            return False
        
        return True
        
    except Exception as e:
        st.error(f"Failed to upload to Google Doc: {str(e)}")
        return False
