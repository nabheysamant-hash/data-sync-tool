import streamlit as st
import pandas as pd
import requests
import time
import json

# --- PAGE SETUP ---
st.set_page_config(page_title="Data Sync Pro (Debug Mode)", page_icon="🐞", layout="centered")

st.title("🐞 Data Sync Pro (Diagnostic)")
st.markdown("Use this version to debug why 'Success' is appearing for bad data.")
st.divider()

# --- SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Configuration")
    url = st.text_input("Endpoint URL", "https://services.onlinesales.ai/merchandiseFeedService/products")
    retailer_id = st.text_input("Retailer ID (x-retailer-id)", "407")
    token = st.text_input("Token (x-token)", type="password")
    batch_size = st.slider("Batch Size", 1, 50, 5) # Default small for debugging
    
    st.divider()
    debug_mode = st.checkbox("Enable Deep Debugging", value=True)
    st.info("Keep 'Deep Debugging' ON to see raw server responses.")

# --- MAIN ---
uploaded_file = st.file_uploader("📂 Upload Source CSV", type=["csv"])

if uploaded_file:
    df = pd.read_csv(uploaded_file, encoding='utf-8-sig')
    df.columns = df.columns.str.strip() # Clean headers
    
    st.subheader("1. Data Preview")
    st.dataframe(df.head(3))
    
    # --- VALIDATION CHECK BEFORE SENDING ---
    required_cols = ['id', 'title']
    missing_cols = [c for c in required_cols if c not in df.columns]
    
    if missing_cols:
        st.error(f"❌ CRITICAL ERROR: Your CSV is missing required columns: {missing_cols}")
        st.warning(" The script uses these columns to build the JSON. If they are missing, it sends NOTHING.")
        st.stop() # Stop execution here

    if st.button("🚀 Start Diagnostic Sync", type="primary"):
        if not token:
            st.error("❌ Missing Token")
        else:
            progress_bar = st.progress(0)
            logs = st.container()
            
            success_count = 0
            error_count = 0
            skipped_count = 0
            
            records = df.to_dict(orient='records')
            total_records = len(records)
            
            # --- LOOP ---
            for i in range(0, total_records, batch_size):
                batch = records[i:i+batch_size]
                
                # 1. Filter Data
                clean_batch = []
                for row in batch:
                    # Logic: Must have ID and Title to be valid
                    if pd.notna(row.get('id')) and pd.notna(row.get('title')):
                         # Remove empty keys
                        clean_row = {k: v for k, v in row.items() if pd.notna(v) and str(v).strip() != ""}
                        clean_batch.append(clean_row)
                    else:
                        skipped_count += 1
                
                if not clean_batch:
                    with logs:
                        st.warning(f"⚠️ Batch {i//batch_size + 1}: Skipped because all rows lacked 'id' or 'title'.")
                    continue

                # 2. Setup Request
                api_headers = {
                    'Content-Type': 'application/json',
                    'x-retailer-id': retailer_id,
                    'x-token': token
                }
                payload = {"products": clean_batch}

                # --- DEBUG: SHOW PAYLOAD ---
                if debug_mode and i == 0:
                    with logs:
                        st.info("🔎 PREVIEW: Here is the exact JSON being sent (First Batch):")
                        st.json(payload)

                # 3. Send Request
                try:
                    resp = requests.post(url, json=payload, headers=api_headers)
                    
                    # --- DEBUG: INSPECT RESPONSE ---
                    server_msg = "No content"
                    try:
                        server_msg = resp.json()
                    except:
                        server_msg = resp.text

                    if resp.status_code == 200:
                        success_count += len(clean_batch)
                        with logs:
                            st.success(f"✅ Batch {i//batch_size + 1} Sent. Server said: {resp.status_code}")
                            if debug_mode:
                                st.json(server_msg) # Show what the server actually returned
                    else:
                        error_count += len(clean_batch)
                        with logs:
                            st.error(f"❌ Batch Failed ({resp.status_code})")
                            st.write(server_msg)

                except Exception as e:
                    error_count += len(clean_batch)
                    st.error(f"Network Error: {e}")
                
                # Update UI
                progress_bar.progress(min((i + batch_size) / total_records, 1.0))
                time.sleep(0.1)

            # --- FINAL REPORT ---
            st.divider()
            if success_count == 0 and error_count == 0:
                 st.error("❌ RESULT: 0 items sent. Your CSV headers likely do not match 'id' and 'title'.")
            else:
                 col1, col2, col3 = st.columns(3)
                 col1.metric("Success", success_count)
                 col2.metric("Failed", error_count)
                 col3.metric("Skipped (Invalid Data)", skipped_count)
