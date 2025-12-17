"""
Google Docs Uploader Module
Automatically uploads market breadth data to Google Docs as a table
"""
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import pandas as pd
import streamlit as st


def get_docs_service():
    """
    Create Google Docs API service using service account credentials from Streamlit secrets

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
    Clear all content from the document

    Args:
        service: Google Docs API service
        doc_id: Document ID
    
    Returns:
        bool: True if successful
    """
    try:
        # Get document to find content length
        doc = service.documents().get(documentId=doc_id).execute()
        content = doc.get('body', {}).get('content', [])
        
        if len(content) <= 1:
            # Document is empty or only has paragraph marker
            return True
        
        # Find the end index of the content (excluding the final newline)
        end_index = content[-1].get('endIndex', 1) - 1
        
        if end_index <= 1:
            return True
        
        # Delete all content except the final newline
        requests = [{
            'deleteContentRange': {
                'range': {
                    'startIndex': 1,
                    'endIndex': end_index
                }
            }
        }]
        
        service.documents().batchUpdate(
            documentId=doc_id,
            body={'requests': requests}
        ).execute()
        
        return True
        
    except Exception as e:
        st.warning(f"Error clearing document: {str(e)}")
        return False


def create_table_requests(df):
    """
    Create requests to insert a table with data from DataFrame

    Args:
        df: DataFrame to convert to table

    Returns:
        list: List of request objects for batchUpdate
    """
    rows = len(df) + 1  # +1 for header
    cols = len(df.columns)
    
    requests = []
    
    # 1. Insert table at index 1 (after the document start)
    requests.append({
        'insertTable': {
            'rows': rows,
            'columns': cols,
            'location': {
                'index': 1
            }
        }
    })
    
    return requests


def populate_table_requests(df, table_start_index):
    """
    Create requests to populate table cells with data
    
    Args:
        df: DataFrame with data
        table_start_index: Starting index of the table in the document
    
    Returns:
        list: List of request objects for batchUpdate
    """
    requests = []
    
    # Calculate cell indices
    # Table structure: each cell has content with paragraph
    # We need to insert text at specific indices
    
    rows = len(df) + 1
    cols = len(df.columns)
    
    # Build data array (header + data rows)
    all_data = [df.columns.tolist()] + df.fillna('').values.tolist()
    
    # Start from the end to avoid index shifting
    # Each cell in a table has structure: startIndex -> paragraph -> text -> endIndex
    # We insert text at cell's startIndex + 2 (after table cell marker and paragraph marker)
    
    current_index = table_start_index + 4  # Start after table element markers
    
    cell_texts = []
    for row_idx, row in enumerate(all_data):
        for col_idx, cell_value in enumerate(row):
            text = str(cell_value) if cell_value != '' else ''
            cell_texts.append({
                'row': row_idx,
                'col': col_idx,
                'text': text
            })
    
    return cell_texts


def upload_to_google_doc(df, doc_id):
    """
    Upload dataframe to Google Doc as a table (overwrites existing content)

    Args:
        df: DataFrame to upload (already formatted with display names and string values)
        doc_id: Google Doc ID

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get Google Docs service
        service = get_docs_service()
        if service is None:
            return False

        # Clear existing content
        if not clear_document(service, doc_id):
            return False

        rows = len(df) + 1  # +1 for header
        cols = len(df.columns)
        
        # Insert table
        insert_table_request = {
            'insertTable': {
                'rows': rows,
                'columns': cols,
                'location': {
                    'index': 1
                }
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
        
        # Build requests to insert text into cells (in reverse order to avoid index shifting)
        text_requests = []
        
        # Prepare all data (header + rows)
        all_data = [df.columns.tolist()] + df.fillna('').values.tolist()
        
        # Collect all cell insertions
        cell_insertions = []
        
        for row_idx, table_row in enumerate(table.get('tableRows', [])):
            for col_idx, cell in enumerate(table_row.get('tableCells', [])):
                # Get the start index of the cell content
                cell_content = cell.get('content', [])
                if cell_content:
                    # Find the paragraph start index
                    para = cell_content[0]
                    if 'paragraph' in para:
                        start_index = para.get('startIndex', 0)
                        
                        # Get the text to insert
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
        for insertion in cell_insertions:
            text_requests.append({
                'insertText': {
                    'location': {
                        'index': insertion['index']
                    },
                    'text': insertion['text']
                }
            })
        
        # Execute text insertions in batches (API has limits)
        batch_size = 100
        for i in range(0, len(text_requests), batch_size):
            batch = text_requests[i:i + batch_size]
            service.documents().batchUpdate(
                documentId=doc_id,
                body={'requests': batch}
            ).execute()
        
        # Format header row (bold)
        # Re-fetch document to get updated indices
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
                                format_requests.append({
                                    'updateTextStyle': {
                                        'range': {
                                            'startIndex': start_idx,
                                            'endIndex': end_idx - 1  # Exclude paragraph marker
                                        },
                                        'textStyle': {
                                            'bold': True
                                        },
                                        'fields': 'bold'
                                    }
                                })
                    
                    if format_requests:
                        try:
                            service.documents().batchUpdate(
                                documentId=doc_id,
                                body={'requests': format_requests}
                            ).execute()
                        except:
                            pass  # Silent fail for formatting
                break

        return True

    except Exception as e:
        st.error(f"Failed to upload to Google Doc: {str(e)}")
        return False
