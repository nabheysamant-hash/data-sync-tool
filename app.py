import streamlit as st
import pandas as pd
import requests
import time
import json

# --- PAGE SETUP ---
st.set_page_config(
    page_title="Data Sync Pro", 
    page_icon="🚀",
    layout="centered"
)

# --- HEADER ---
st.title("🚀 Data Sync Pro")
st.markdown("Upload your CSV and sync products to the merchandise feed.")
st.divider()

# --- SIDEBAR CONFIGURATION ---
with st.sidebar:
    st.header("⚙️ Configuration")
    
    url = st.text_input(
        "Endpoint URL", 
        value="https://services.onlinesales.ai/merchandiseFeedService/products"
    )
    
    retailer_id = st.text_input(
        "Retailer ID (x-retailer-id)", 
        value="407",
        help="The numeric ID found in your dashboard."
    )
    
    token = st.text_input(
        "Token (x-token)", 
        type="password",
        help="Your secret API access token."
    )
    
    batch_size = st.slider("Batch Size", min_value=10, max_value=200, value=50)

# --- MAIN INTERFACE ---
uploaded_file = st.file_uploader("📂 Upload Source CSV", type=["csv"])

if uploaded_file:
    # Preview Data
    try:
        df = pd.read_csv(uploaded_file, encoding='utf-8-sig')
        # Clean headers: remove spaces
        df.columns = df.columns.str.strip()
        
        st.write(f"**Preview ({len(df)} rows detected):**")
        st.dataframe(df.head(3), use_container_width=True)
        
        # --- ACTION BUTTON ---
        if st.button("🚀 Start Synchronization", type="primary"):
            if not token:
                st.error("❌ API Token is missing!")
            else:
                # Initialize Progress
                progress_bar = st.progress(0)
                status_text = st.empty()
                logs_expander = st.expander("View Transaction Logs", expanded=True)
                
                success_count = 0
                error_count = 0
                
                # Prepare Data
                records = df.to_dict(orient='records')
                total_records = len(records)
                
                # --- PROCESSING LOOP ---
                for i in range(0, total_records, batch_size):
                    batch = records[i:i+batch_size]
                    
                    # 1. Clean Batch (Remove NaN/Empty)
                    clean_batch = []
                    for row in batch:
                        # Only keep fields that are not empty/null
                        clean_row = {k: v for k, v in row.items() if pd.notna(v) and str(v).strip() != ""}
                        # Skip rows without ID or Title
                        if 'id' in clean_row and 'title' in clean_row:
                            clean_batch.append(clean_row)
                    
                    if not clean_batch:
                        continue

                    # 2. Prepare Headers & Payload
                    api_headers = {
                        'Content-Type': 'application/json',
                        'x-retailer-id': retailer_id,
                        'x-token': token
                    }
                    payload = {"products": clean_batch}

                    # 3. Send Request
                    try:
                        resp = requests.post(url, json=payload, headers=api_headers)
                        
                        if resp.status_code == 200:
                            success_count += len(clean_batch)
                            logs_expander.write(f"✅ Batch {i//batch_size + 1}: Success ({len(clean_batch)} items)")
                        elif resp.status_code == 401:
                            error_count += len(clean_batch)
                            logs_expander.error("❌ 401 Unauthorized - Check Credentials")
                            break # Stop on auth error
                        else:
                            error_count += len(clean_batch)
                            logs_expander.warning(f"⚠️ Error {resp.status_code}: {resp.text[:50]}...")
                            
                    except Exception as e:
                        error_count += len(clean_batch)
                        logs_expander.error(f"❌ Network Error: {e}")

                    # 4. Update Progress
                    current_progress = min((i + batch_size) / total_records, 1.0)
                    progress_bar.progress(current_progress)
                    status_text.caption(f"Processed: {i + len(batch)} / {total_records}")
                    time.sleep(0.1) # Slight throttle

                # --- SUMMARY ---
                st.success("Job Complete!")
                col1, col2 = st.columns(2)
                col1.metric("Successfully Sent", success_count)
                col2.metric("Failed", error_count)

    except Exception as e:
        st.error(f"Error reading file: {e}")

else:
    st.info("👆 Upload a CSV file to get started.")