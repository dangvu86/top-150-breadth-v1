"""
Google Docs Uploader Module
Uploads market breadth data to Google Docs as formatted text (tab-separated columns)

Strategy: Clear document → Insert formatted text (no complex table structure)
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
        credentials_dict = dict(st.secrets["gcp_service_account"])

        scope = [
            'https://www.googleapis.com/auth/documents',
            'https://www.googleapis.com/auth/drive'
        ]

        credentials = Credentials.from_service_account_info(
            credentials_dict,
            scopes=scope
        )

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
        doc = service.documents().get(documentId=doc_id).execute()
        content = doc.get('body', {}).get('content', [])

        if not content or len(content) <= 1:
            return True

        # Find the end index
        end_index = 1
        for element in content:
            elem_end = element.get('endIndex', 1)
            if elem_end > end_index:
                end_index = elem_end

        delete_end = end_index - 1

        if delete_end <= 1:
            return True

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
        if 'Invalid' in error_str and 'range' in error_str.lower():
            return True
        st.warning(f"Error clearing document: {error_str}")
        return False


def dataframe_to_text(df):
    """
    Convert DataFrame to tab-separated text with aligned columns

    Args:
        df: DataFrame to convert

    Returns:
        str: Formatted text
    """
    # Get all data including header
    header = df.columns.tolist()
    rows = df.fillna('').values.tolist()

    # Calculate column widths
    col_widths = []
    for i, col in enumerate(header):
        max_width = len(str(col))
        for row in rows:
            if i < len(row):
                max_width = max(max_width, len(str(row[i])))
        col_widths.append(max_width + 2)  # Add padding

    # Build formatted text
    lines = []

    # Header line
    header_line = ""
    for i, col in enumerate(header):
        header_line += str(col).ljust(col_widths[i])
    lines.append(header_line)

    # Separator line
    separator = "-" * sum(col_widths)
    lines.append(separator)

    # Data rows
    for row in rows:
        row_line = ""
        for i, cell in enumerate(row):
            if i < len(col_widths):
                row_line += str(cell).ljust(col_widths[i])
        lines.append(row_line)

    return "\n".join(lines)


def upload_to_google_doc(df, doc_id):
    """
    Upload dataframe to Google Doc as formatted text

    Args:
        df: DataFrame to upload
        doc_id: Google Doc ID

    Returns:
        bool: True if successful
    """
    try:
        service = get_docs_service()
        if service is None:
            return False

        # Step 1: Clear existing content
        if not clear_document(service, doc_id):
            return False

        # Step 2: Convert dataframe to formatted text
        text_content = dataframe_to_text(df)

        # Step 3: Insert text at beginning of document
        requests = [{
            'insertText': {
                'location': {'index': 1},
                'text': text_content
            }
        }]

        service.documents().batchUpdate(
            documentId=doc_id,
            body={'requests': requests}
        ).execute()

        # Step 4: Format header (first line) as bold
        try:
            # Find the end of first line
            first_newline = text_content.find('\n')
            if first_newline > 0:
                format_requests = [{
                    'updateTextStyle': {
                        'range': {
                            'startIndex': 1,
                            'endIndex': first_newline + 1
                        },
                        'textStyle': {'bold': True},
                        'fields': 'bold'
                    }
                }]

                service.documents().batchUpdate(
                    documentId=doc_id,
                    body={'requests': format_requests}
                ).execute()
        except:
            pass  # Silent fail for formatting

        return True

    except Exception as e:
        st.error(f"Failed to upload to Google Doc: {str(e)}")
        return False
