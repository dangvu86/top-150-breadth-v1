import streamlit as st
import pandas as pd
import sys
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Add modules to path
sys.path.append(os.path.dirname(__file__))

from modules.data_loader import load_vnindex_data, load_price_volume_data
from modules.indicators import calculate_all_indicators
from modules.winrate_api import fetch_winrate_data, fetch_breakout_data
from modules.google_sheet_uploader import upload_to_google_sheet, format_google_sheet
from modules.google_docs_uploader import upload_to_google_doc

# Page config
st.set_page_config(
    page_title="Market Breadth Analysis",
    page_icon="📊",
    layout="wide"
)



# Load data - no cache, always reload when app restarts
def load_data():
    progress_bar = st.progress(0)

    # Load VNINDEX
    progress_bar.progress(25)
    df_vnindex = load_vnindex_data()

    # Load stocks
    progress_bar.progress(50)
    df_stocks = load_price_volume_data()

    progress_bar.progress(100)

    # Clear progress bar after completion
    import time
    time.sleep(0.3)
    progress_bar.empty()

    return df_vnindex, df_stocks

def compute_indicators(df_vnindex, df_stocks):
    progress_bar = st.progress(0)

    progress_bar.progress(50)
    result = calculate_all_indicators(df_vnindex, df_stocks)

    progress_bar.progress(100)

    # Clear progress bar after completion
    import time
    time.sleep(0.3)
    progress_bar.empty()

    return result

# Load and calculate
df_vnindex, df_stocks = load_data()
df_result = compute_indicators(df_vnindex, df_stocks)

# Load WinRate data from API
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_winrate_data():
    # Get bearer token from secrets (fallback to empty string for local testing)
    try:
        bearer_token = st.secrets.get("DRAGON_CAPITAL_TOKEN", "")
    except:
        bearer_token = ""

    if bearer_token:
        df_winrate = fetch_winrate_data(bearer_token)
        return df_winrate
    else:
        # Return empty DataFrame if no token
        return pd.DataFrame(columns=['date', 'winRate'])

df_winrate = load_winrate_data()

# Load BreakOut data from API
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_breakout_data():
    # Get bearer token from secrets (fallback to empty string for local testing)
    try:
        bearer_token = st.secrets.get("DRAGON_CAPITAL_TOKEN", "")
    except:
        bearer_token = ""

    if bearer_token:
        df_breakout = fetch_breakout_data(bearer_token)
        return df_breakout
    else:
        # Return empty DataFrame if no token
        return pd.DataFrame(columns=['date', 'breakOut'])

df_breakout = load_breakout_data()

# Merge WinRate data with result
if not df_winrate.empty:
    # Rename winRate column
    df_winrate = df_winrate.rename(columns={'winRate': 'New_High_WinRate'})

    # Convert to percentage
    df_winrate['New_High_WinRate'] = df_winrate['New_High_WinRate'] * 100

    # Ensure both date columns are datetime without timezone
    df_winrate['date'] = pd.to_datetime(df_winrate['date']).dt.tz_localize(None)
    df_result['Trading Date'] = pd.to_datetime(df_result['Trading Date']).dt.tz_localize(None)

    # Merge on date
    df_result = df_result.merge(
        df_winrate[['date', 'New_High_WinRate']],
        left_on='Trading Date',
        right_on='date',
        how='left'
    )

    # Drop the extra date column
    df_result = df_result.drop(columns=['date'])

# Merge BreakOut data with result
if not df_breakout.empty:
    # Rename breakOut column
    df_breakout = df_breakout.rename(columns={'breakOut': 'Break_Out'})

    # Convert to percentage
    df_breakout['Break_Out'] = df_breakout['Break_Out'] * 100

    # Ensure both date columns are datetime without timezone
    df_breakout['date'] = pd.to_datetime(df_breakout['date']).dt.tz_localize(None)
    df_result['Trading Date'] = pd.to_datetime(df_result['Trading Date']).dt.tz_localize(None)

    # Merge on date
    df_result = df_result.merge(
        df_breakout[['date', 'Break_Out']],
        left_on='Trading Date',
        right_on='date',
        how='left'
    )

    # Drop the extra date column
    df_result = df_result.drop(columns=['date'])

# Filters
st.sidebar.header("Filters")

# Start Date
start_date = st.sidebar.date_input(
    "Start Date",
    value=df_result['Trading Date'].min().date(),
    min_value=df_result['Trading Date'].min().date(),
    max_value=df_result['Trading Date'].max().date()
)

# End Date
end_date = st.sidebar.date_input(
    "End Date",
    value=df_result['Trading Date'].max().date(),
    min_value=df_result['Trading Date'].min().date(),
    max_value=df_result['Trading Date'].max().date()
)

# Filter data
df_filtered = df_result[
    (df_result['Trading Date'].dt.date >= start_date) &
    (df_result['Trading Date'].dt.date <= end_date)
].copy()

# Calculate Avg RSI, Breadth
df_filtered['Avg_RSI_Breadth'] = (
    df_filtered['VnIndex_RSI_21'] +
    df_filtered['VnIndex_RSI_70'] +
    df_filtered['MFI_15D_RSI_21'] +
    df_filtered['NHNL_15D_RSI_21'] +
    df_filtered['AD_15D_RSI_21'] +
    df_filtered['Breadth_Above_MA50']
) / 6

# Display data table
st.header("Market Breadth Indicators")

# Define display columns in specific order
display_columns = [
    'Trading Date',
    'VnIndex',
    'VnIndex_RSI_21',
    'VnIndex_RSI_70',
    'Breadth_Above_MA50',
    'NHNL_15D_RSI_21',  # NHNL RSI after Breadth
    'MFI_15D_RSI_21',   # MFI RSI
    'AD_15D_RSI_21',    # AD RSI
    'Avg_RSI_Breadth',  # Score after RSI columns
    'MFI_15D_Sum',  # MFI
    'AD_15D_Sum',   # AD
    'NHNL_15D_Sum', # NHNL
    'New_High_WinRate',  # New High column
    'Break_Out',  # Break Out column
    # Remaining columns
    'Breadth_20D_Avg',
    'MFI_Up_Value',
    'MFI_Down_Value',
    'MFI_20D_Avg',
    'AD_Advances',
    'AD_Declines',
    'AD_Net',
    'AD_20D_Avg',
    'NHNL_New_Highs',
    'NHNL_New_Lows',
    'NHNL_Net',
    'NHNL_20D_Avg'
]

# Filter only columns that exist in df_filtered
display_columns = [col for col in display_columns if col in df_filtered.columns]

# Prepare display columns
display_df = df_filtered[display_columns].copy()

# Build column name mapping
column_mapping = {
    'Trading Date': 'Date',
    'VnIndex': 'VnIndex',
    'VnIndex_RSI_21': 'VNI RSI21',
    'VnIndex_RSI_70': 'VNI RSI70',
    'Breadth_Above_MA50': 'Breadth - % > MA50',
    'Avg_RSI_Breadth': 'Score',
    'MFI_15D_RSI_21': 'MFI RSI',
    'AD_15D_RSI_21': 'A/D RSI',
    'NHNL_15D_RSI_21': 'NHNL RSI',
    'New_High_WinRate': 'New High',
    'Break_Out': 'Break Out',
    'MFI_15D_Sum': 'MFI',
    'AD_15D_Sum': 'AD',
    'NHNL_15D_Sum': 'NHNL',
    'Breadth_20D_Avg': '20D Avg Breadth',
    'MFI_Up_Value': 'MFI: Up Value',
    'MFI_Down_Value': 'MFI: Down Value',
    'MFI_20D_Avg': '20D Avg MFI',
    'AD_Advances': 'A/D: Advances',
    'AD_Declines': 'A/D: Declines',
    'AD_Net': 'A/D: Net (A-B)',
    'AD_20D_Avg': '20D Avg A/D',
    'NHNL_New_Highs': 'NHNL: New Highs',
    'NHNL_New_Lows': 'NHNL: New Lows',
    'NHNL_Net': 'NHNL: Net (A-B)',
    'NHNL_20D_Avg': '20D Avg NHNL'
}

# Rename columns for display
display_df = display_df.rename(columns=column_mapping)

# Format date
display_df['Date'] = pd.to_datetime(display_df['Date']).dt.strftime('%Y-%m-%d')

# Format MFI columns in billions (only if columns exist)
if 'MFI: Up Value' in display_df.columns:
    display_df['MFI: Up Value'] = display_df['MFI: Up Value'].apply(lambda x: f"{x/1000000000:,.0f}" if pd.notna(x) else "")
if 'MFI: Down Value' in display_df.columns:
    display_df['MFI: Down Value'] = display_df['MFI: Down Value'].apply(lambda x: f"{x/1000000000:,.0f}" if pd.notna(x) else "")
if 'MFI' in display_df.columns:
    display_df['MFI'] = display_df['MFI'].apply(lambda x: f"{x/1000000000:,.0f}" if pd.notna(x) else "")
if '20D Avg MFI' in display_df.columns:
    display_df['20D Avg MFI'] = display_df['20D Avg MFI'].apply(lambda x: f"{x/1000000000:,.0f}" if pd.notna(x) else "")

# Sort by date descending
display_df = display_df.sort_values('Date', ascending=False)

# Auto-upload to Google Sheet (if configured)
try:
    # Get Google Sheet ID from secrets
    if "GOOGLE_SHEET_ID" in st.secrets:
        sheet_id = st.secrets["GOOGLE_SHEET_ID"]

        # Prepare formatted data for upload - only export columns up to A/D RSI
        export_columns = ['Date', 'VnIndex', 'VNI RSI21', 'VNI RSI70', 'Breadth - % > MA50', 'NHNL RSI', 'MFI RSI', 'A/D RSI']
        # Filter only columns that exist
        export_columns = [col for col in export_columns if col in display_df.columns]
        df_upload = display_df[export_columns].copy()

        # Format numeric columns with 1 decimal place
        numeric_1decimal_cols = ['VnIndex', 'VNI RSI21', 'VNI RSI70', 'MFI RSI', 'A/D RSI', 'NHNL RSI']
        for col in numeric_1decimal_cols:
            if col in df_upload.columns:
                df_upload[col] = df_upload[col].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "")

        # Format percentage columns with 1 decimal place and % sign
        percentage_cols = ['Breadth - % > MA50']
        for col in percentage_cols:
            if col in df_upload.columns:
                df_upload[col] = df_upload[col].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "")

        # Show upload status in sidebar
        with st.sidebar:
            with st.spinner("Uploading to Google Sheet..."):
                success = upload_to_google_sheet(df_upload, sheet_id)
                if success:
                    st.success("✅ Data uploaded to Google Sheet")
                    format_google_sheet(sheet_id)
                else:
                    st.warning("⚠️ Failed to upload to Google Sheet - check logs")
except Exception as e:
    with st.sidebar:
        st.error(f"❌ Google Sheet upload error: {str(e)}")

# Auto-upload to Google Doc (if configured)
try:
    if "GOOGLE_DOC_FOLDER_ID" in st.secrets:
        folder_id = st.secrets["GOOGLE_DOC_FOLDER_ID"]
        doc_name = st.secrets.get("GOOGLE_DOC_NAME", "Market Breadth Data")

        # Prepare formatted data for upload
        export_columns = ['Date', 'VnIndex', 'VNI RSI21', 'VNI RSI70', 'Breadth - % > MA50', 'NHNL RSI', 'MFI RSI', 'A/D RSI']
        export_columns = [col for col in export_columns if col in display_df.columns]
        df_upload_doc = display_df[export_columns].copy()

        # Format numeric columns
        numeric_1decimal_cols = ['VnIndex', 'VNI RSI21', 'VNI RSI70', 'MFI RSI', 'A/D RSI', 'NHNL RSI']
        for col in numeric_1decimal_cols:
            if col in df_upload_doc.columns:
                df_upload_doc[col] = df_upload_doc[col].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "")

        # Format percentage columns
        percentage_cols = ['Breadth - % > MA50']
        for col in percentage_cols:
            if col in df_upload_doc.columns:
                df_upload_doc[col] = df_upload_doc[col].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "")

        # Show upload status in sidebar
        with st.sidebar:
            with st.spinner("Uploading to Google Doc..."):
                success = upload_to_google_doc(df_upload_doc, folder_id, doc_name)
                if success:
                    st.success("✅ Data uploaded to Google Doc")
                else:
                    st.warning("⚠️ Failed to upload to Google Doc - check logs")
except Exception as e:
    with st.sidebar:
        st.error(f"❌ Google Doc upload error: {str(e)}")

# Build column config dynamically
column_config = {
    'Date': st.column_config.TextColumn('Date'),
    'VnIndex': st.column_config.NumberColumn('VnIndex', format="%.1f"),
    'VNI RSI21': st.column_config.NumberColumn('VNI RSI21', format="%.1f"),
    'VNI RSI70': st.column_config.NumberColumn('VNI RSI70', format="%.1f"),
    'Breadth - % > MA50': st.column_config.NumberColumn('Breadth - % > MA50', format="%.1f%%"),
    'Score': st.column_config.NumberColumn('Score', format="%.1f"),
    'MFI RSI': st.column_config.NumberColumn('MFI RSI', format="%.1f"),
    'A/D RSI': st.column_config.NumberColumn('A/D RSI', format="%.1f"),
    'NHNL RSI': st.column_config.NumberColumn('NHNL RSI', format="%.1f"),
    'New High': st.column_config.NumberColumn('New High', format="%.1f%%"),
    'Break Out': st.column_config.NumberColumn('Break Out', format="%.1f%%"),
    'MFI': st.column_config.TextColumn('MFI'),
    'AD': st.column_config.NumberColumn('AD', format="%d"),
    'NHNL': st.column_config.NumberColumn('NHNL', format="%d"),
    '20D Avg Breadth': st.column_config.NumberColumn('20D Avg Breadth', format="%.1f%%"),
    'MFI: Up Value': st.column_config.TextColumn('MFI: Up Value'),
    'MFI: Down Value': st.column_config.TextColumn('MFI: Down Value'),
    '20D Avg MFI': st.column_config.TextColumn('20D Avg MFI'),
    'A/D: Advances': st.column_config.NumberColumn('A/D: Advances', format="%d"),
    'A/D: Declines': st.column_config.NumberColumn('A/D: Declines', format="%d"),
    'A/D: Net (A-B)': st.column_config.NumberColumn('A/D: Net (A-B)', format="%d"),
    '20D Avg A/D': st.column_config.NumberColumn('20D Avg A/D', format="%.1f"),
    'NHNL: New Highs': st.column_config.NumberColumn('NHNL: New Highs', format="%d"),
    'NHNL: New Lows': st.column_config.NumberColumn('NHNL: New Lows', format="%d"),
    'NHNL: Net (A-B)': st.column_config.NumberColumn('NHNL: Net (A-B)', format="%d"),
    '20D Avg NHNL': st.column_config.NumberColumn('20D Avg NHNL', format="%.1f"),
}

# Display table with NumberColumn for right alignment and formatting
st.dataframe(
    display_df,
    use_container_width=True,
    height=900,  # Height for ~25 rows
    column_config=column_config,
    hide_index=True
)

# Download button - export with same format as display
# Create export dataframe from display_df (already sorted by date descending)
df_export = display_df.copy()

# Apply formatting to match display table
# Format numeric columns with 1 decimal place
numeric_1decimal_cols = ['VnIndex', 'VNI RSI21', 'VNI RSI70', 'Score', 'MFI RSI', 'A/D RSI', 'NHNL RSI', '20D Avg A/D', '20D Avg NHNL']
for col in numeric_1decimal_cols:
    if col in df_export.columns:
        df_export[col] = df_export[col].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "")

# Format percentage columns with 1 decimal place and % sign
percentage_cols = ['Breadth - % > MA50', 'New High', 'Break Out', '20D Avg Breadth']
for col in percentage_cols:
    if col in df_export.columns:
        df_export[col] = df_export[col].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "")

# Format integer columns (no decimal places)
integer_cols = ['AD', 'NHNL', 'A/D: Advances', 'A/D: Declines', 'A/D: Net (A-B)', 'NHNL: New Highs', 'NHNL: New Lows', 'NHNL: Net (A-B)']
for col in integer_cols:
    if col in df_export.columns:
        df_export[col] = df_export[col].apply(lambda x: f"{int(x)}" if pd.notna(x) else "")

csv = df_export.to_csv(index=False)
st.download_button(
    label="📥 Download Full Data as CSV",
    data=csv,
    file_name="market_breadth_analysis.csv",
    mime="text/csv"
)

# Charts Section
st.header("Technical Charts")

# Prepare data for charts (use filtered data)
chart_data = df_filtered.sort_values('Trading Date')

# Chart 1: VnIndex and VnIndex RSI
st.subheader("VnIndex & RSI")
fig1 = make_subplots(
    rows=2, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.05,
    row_heights=[0.6, 0.4]
)

fig1.add_trace(
    go.Scatter(x=chart_data['Trading Date'], y=chart_data['VnIndex'],
               name='VnIndex', line=dict(color='blue', width=2)),
    row=1, col=1
)

fig1.add_trace(
    go.Scatter(x=chart_data['Trading Date'], y=chart_data['VnIndex_RSI_21'],
               name='VnIndex RSI', line=dict(color='purple', width=2)),
    row=2, col=1
)

# Add RSI reference lines
fig1.add_hline(y=70, line_dash="dot", line_color="red", line_width=1, row=2, col=1)
fig1.add_hline(y=30, line_dash="dot", line_color="green", line_width=1, row=2, col=1)

fig1.update_xaxes(title_text="Date", row=2, col=1)
fig1.update_yaxes(title_text="VnIndex", row=1, col=1)
fig1.update_yaxes(title_text="RSI", row=2, col=1, range=[0, 100])
fig1.update_layout(height=600, showlegend=False)

st.plotly_chart(fig1, use_container_width=True)

# Chart 2: VnIndex and Score
st.subheader("VnIndex & Score")
fig_score = make_subplots(
    rows=2, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.05,
    row_heights=[0.6, 0.4]
)

fig_score.add_trace(
    go.Scatter(x=chart_data['Trading Date'], y=chart_data['VnIndex'],
               name='VnIndex', line=dict(color='blue', width=2)),
    row=1, col=1
)

fig_score.add_trace(
    go.Scatter(x=chart_data['Trading Date'], y=chart_data['Avg_RSI_Breadth'],
               name='Score', line=dict(color='darkblue', width=2)),
    row=2, col=1
)

# Add Score reference lines (similar to RSI)
fig_score.add_hline(y=70, line_dash="dot", line_color="red", line_width=1, row=2, col=1)
fig_score.add_hline(y=30, line_dash="dot", line_color="green", line_width=1, row=2, col=1)

fig_score.update_xaxes(title_text="Date", row=2, col=1)
fig_score.update_yaxes(title_text="VnIndex", row=1, col=1)
fig_score.update_yaxes(title_text="Score", row=2, col=1, range=[0, 100])
fig_score.update_layout(height=600, showlegend=False)

st.plotly_chart(fig_score, use_container_width=True)

# Chart 3: VnIndex and Breadth % Above MA50
st.subheader("VnIndex & Breadth % > MA50")
fig_breadth = make_subplots(
    rows=2, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.05,
    row_heights=[0.6, 0.4]
)

fig_breadth.add_trace(
    go.Scatter(x=chart_data['Trading Date'], y=chart_data['VnIndex'],
               name='VnIndex', line=dict(color='blue', width=2)),
    row=1, col=1
)

fig_breadth.add_trace(
    go.Scatter(x=chart_data['Trading Date'], y=chart_data['Breadth_Above_MA50'],
               name='Breadth % > MA50', line=dict(color='teal', width=2)),
    row=2, col=1
)

fig_breadth.update_xaxes(title_text="Date", row=2, col=1)
fig_breadth.update_yaxes(title_text="VnIndex", row=1, col=1)
fig_breadth.update_yaxes(title_text="Breadth %", row=2, col=1)
fig_breadth.update_layout(height=600, showlegend=False)

st.plotly_chart(fig_breadth, use_container_width=True)

# Chart 4: MFI and MFI RSI
st.subheader("MFI & RSI")
fig2 = make_subplots(
    rows=2, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.05,
    row_heights=[0.6, 0.4]
)

fig2.add_trace(
    go.Scatter(x=chart_data['Trading Date'], y=chart_data['MFI_15D_Sum'],
               name='MFI', line=dict(color='green', width=2)),
    row=1, col=1
)

if 'MFI_15D_RSI_21' in chart_data.columns:
    fig2.add_trace(
        go.Scatter(x=chart_data['Trading Date'], y=chart_data['MFI_15D_RSI_21'],
                   name='MFI RSI', line=dict(color='purple', width=2)),
        row=2, col=1
    )

    # Add RSI reference lines
    fig2.add_hline(y=70, line_dash="dot", line_color="red", line_width=1, row=2, col=1)
    fig2.add_hline(y=30, line_dash="dot", line_color="green", line_width=1, row=2, col=1)

fig2.update_xaxes(title_text="Date", row=2, col=1)
fig2.update_yaxes(title_text="MFI", row=1, col=1)
fig2.update_yaxes(title_text="RSI", row=2, col=1, range=[0, 100])
fig2.update_layout(height=600, showlegend=False)

st.plotly_chart(fig2, use_container_width=True)

# Chart 5: A/D and A/D RSI
st.subheader("Advance/Decline & RSI")
fig3 = make_subplots(
    rows=2, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.05,
    row_heights=[0.6, 0.4]
)

fig3.add_trace(
    go.Scatter(x=chart_data['Trading Date'], y=chart_data['AD_15D_Sum'],
               name='A/D', line=dict(color='orange', width=2)),
    row=1, col=1
)

if 'AD_15D_RSI_21' in chart_data.columns:
    fig3.add_trace(
        go.Scatter(x=chart_data['Trading Date'], y=chart_data['AD_15D_RSI_21'],
                   name='A/D RSI', line=dict(color='purple', width=2)),
        row=2, col=1
    )

    # Add RSI reference lines
    fig3.add_hline(y=70, line_dash="dot", line_color="red", line_width=1, row=2, col=1)
    fig3.add_hline(y=30, line_dash="dot", line_color="green", line_width=1, row=2, col=1)

fig3.update_xaxes(title_text="Date", row=2, col=1)
fig3.update_yaxes(title_text="A/D", row=1, col=1)
fig3.update_yaxes(title_text="RSI", row=2, col=1, range=[0, 100])
fig3.update_layout(height=600, showlegend=False)

st.plotly_chart(fig3, use_container_width=True)

# Chart 6: NHNL and NHNL RSI
st.subheader("New High/New Low & RSI")
fig4 = make_subplots(
    rows=2, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.05,
    row_heights=[0.6, 0.4]
)

fig4.add_trace(
    go.Scatter(x=chart_data['Trading Date'], y=chart_data['NHNL_15D_Sum'],
               name='NHNL', line=dict(color='red', width=2)),
    row=1, col=1
)

if 'NHNL_15D_RSI_21' in chart_data.columns:
    fig4.add_trace(
        go.Scatter(x=chart_data['Trading Date'], y=chart_data['NHNL_15D_RSI_21'],
                   name='NHNL RSI', line=dict(color='purple', width=2)),
        row=2, col=1
    )

    # Add RSI reference lines
    fig4.add_hline(y=70, line_dash="dot", line_color="red", line_width=1, row=2, col=1)
    fig4.add_hline(y=30, line_dash="dot", line_color="green", line_width=1, row=2, col=1)

fig4.update_xaxes(title_text="Date", row=2, col=1)
fig4.update_yaxes(title_text="NHNL", row=1, col=1)
fig4.update_yaxes(title_text="RSI", row=2, col=1, range=[0, 100])
fig4.update_layout(height=600, showlegend=False)

st.plotly_chart(fig4, use_container_width=True)

# Chart 7: VnIndex and New High WinRate
if 'New_High_WinRate' in chart_data.columns:
    st.subheader("VnIndex & New High")
    fig5 = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.6, 0.4]
    )

    fig5.add_trace(
        go.Scatter(x=chart_data['Trading Date'], y=chart_data['VnIndex'],
                   name='VnIndex', line=dict(color='blue', width=2)),
        row=1, col=1
    )

    fig5.add_trace(
        go.Scatter(x=chart_data['Trading Date'], y=chart_data['New_High_WinRate'],
                   name='New High', line=dict(color='darkgreen', width=2)),
        row=2, col=1
    )

    fig5.update_xaxes(title_text="Date", row=2, col=1)
    fig5.update_yaxes(title_text="VnIndex", row=1, col=1)
    fig5.update_yaxes(title_text="New High %", row=2, col=1)
    fig5.update_layout(height=600, showlegend=False)

    st.plotly_chart(fig5, use_container_width=True)

# Chart 8: VnIndex and Break Out
if 'Break_Out' in chart_data.columns:
    st.subheader("VnIndex & Break Out")
    fig6 = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.6, 0.4]
    )

    fig6.add_trace(
        go.Scatter(x=chart_data['Trading Date'], y=chart_data['VnIndex'],
                   name='VnIndex', line=dict(color='blue', width=2)),
        row=1, col=1
    )

    fig6.add_trace(
        go.Scatter(x=chart_data['Trading Date'], y=chart_data['Break_Out'],
                   name='Break Out', line=dict(color='darkorange', width=2)),
        row=2, col=1
    )

    fig6.update_xaxes(title_text="Date", row=2, col=1)
    fig6.update_yaxes(title_text="VnIndex", row=1, col=1)
    fig6.update_yaxes(title_text="Break Out %", row=2, col=1)
    fig6.update_layout(height=600, showlegend=False)

    st.plotly_chart(fig6, use_container_width=True)

# Info
st.sidebar.header("About")
st.sidebar.info("""
**Indicators:**
- **VnIndex RSI (21D)**: 21-day Relative Strength Index
- **Breadth % Above MA50**: % of stocks trading above their 50-day MA
- **Money Flow Index**: Net value of stocks with >1% / <-1% change, 15-day rolling sum
- **Advance/Decline**: Net count of stocks with >1% / <-1% change, 15-day rolling sum
- **New High/New Low**: Net count of stocks at 20-day high vs 20-day low, 15-day rolling sum
- **20D Averages**: 20-day moving averages of respective indicators
""")

st.sidebar.info(f"""
**Data Info:**
- Total Records: {len(df_result)}
- Date Range: {df_result['Trading Date'].min().strftime('%Y-%m-%d')} to {df_result['Trading Date'].max().strftime('%Y-%m-%d')}
- Number of Stocks: {df_stocks['TICKER'].nunique()}
""")
