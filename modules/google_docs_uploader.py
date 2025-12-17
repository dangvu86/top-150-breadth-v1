"""
Google Docs Uploader Module
Automatically uploads market breadth data to Google Docs as a table

Strategy: Clear existing document content → Insert new table
Uses doc_id directly, no new file creation.
"""
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import pandas as pd
import streamlit as st


def get_docs_service():
    """
    Create Google Docs API service using service account credentials

    Returns:
        googleapiclient.discovery.Resource: Google Docs API service
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

        # Build and return the Docs service
        service = build('docs', 'v1', credentials=credentials)
        return service

    except Exception as e:
        st.warning(f"Cannot connect to Google Docs: {str(e)}")
        return None


def clear_document(service, doc_id):
    """
    Clear all content from the document (including tables)

    Args:
        service: Google Docs API service
        doc_id: Document ID
    
    Returns:
        bool: True if successful or document is empty
    """
    try:
        # Get document content
        doc = service.documents().get(documentId=doc_id).execute()
        content = doc.get('body', {}).get('content', [])
        
        if not content or len(content) <= 1:
            # Document is empty or only has paragraph marker
            return True
        
        # Find the end index of content
        # The last element's endIndex is where content ends
        end_index = 1
        for element in content:
            elem_end = element.get('endIndex', 1)
            if elem_end > end_index:
                end_index = elem_end
        
        # Need to delete from 1 to end_index - 1 (preserve final newline)
        delete_end = end_index - 1
        
        if delete_end <= 1:
            # Nothing to delete
            return True
        
        # Delete all content
        requests = [{
            'deleteContentRange': {
                'range': {
                    'startIndex': 1,
                    'endIndex': delete_end
                }
            }
        }]
        
        service.documents().batchUpdate(
            documentId=doc_id,
            body={'requests': requests}
        ).execute()
        
        return True
        
    except Exception as e:
        error_str = str(e)
        # If error is about invalid range, document might be empty - that's OK
        if 'Invalid' in error_str and 'range' in error_str.lower():
            return True
        st.warning(f"Error clearing document: {error_str}")
        return False


def insert_table_with_data(service, doc_id, df):
    """
    Insert a table with data into a Google Doc

    Args:
        service: Google Docs API service
        doc_id: Document ID
        df: DataFrame to insert

    Returns:
        bool: True if successful
    """
    try:
        rows = len(df) + 1  # +1 for header
        cols = len(df.columns)

        # Insert table at beginning of document
        insert_table_request = {
            'insertTable': {
                'rows': rows,
                'columns': cols,
                'location': {'index': 1}
            }
        }

        service.documents().batchUpdate(
            documentId=doc_id,
            body={'requests': [insert_table_request]}
        ).execute()

        # Get the document to find table cell indices
        doc = service.documents().get(documentId=doc_id).execute()
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

        # Sort by index descending (insert from end to start to avoid index shifting)
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
            service.documents().batchUpdate(
                documentId=doc_id,
                body={'requests': batch}
            ).execute()

        # Format header row (bold)
        try:
            doc = service.documents().get(documentId=doc_id).execute()
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
                            service.documents().batchUpdate(
                                documentId=doc_id,
                                body={'requests': format_requests}
                            ).execute()
                    break
        except:
            pass  # Silent fail for formatting

        return True

    except Exception as e:
        st.error(f"Error inserting table: {str(e)}")
        return False


def upload_to_google_doc(df, doc_id):
    """
    Upload dataframe to Google Doc as a table (overwrites existing content)

    Args:
        df: DataFrame to upload
        doc_id: Google Doc ID

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get Google Docs service
        service = get_docs_service()
        if service is None:
            return False

        # Step 1: Clear existing content
        if not clear_document(service, doc_id):
            return False

        # Step 2: Insert table with data
        if not insert_table_with_data(service, doc_id, df):
            return False

        return True

    except Exception as e:
        st.error(f"Failed to upload to Google Doc: {str(e)}")
        return False
