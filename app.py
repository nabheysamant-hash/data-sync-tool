import streamlit as st
import pandas as pd
import requests
import time
import json

# --- PAGE SETUP ---
st.set_page_config(page_title="Data Sync Pro", page_icon="🔁", layout="centered")

st.title("🔁 Data Sync Pro")
st.markdown("Sync your CSV data to the OnlineSales.ai merchandise feed.")
st.divider()

# --- SIDEBAR CONFIGURATION ---
with st.sidebar:
    st.header("⚙️ Connection Settings")
    
    url = st.text_input(
        "Endpoint URL", 
        value="https://services.onlinesales.ai/merchandiseFeedService/products"
    )
    
    col1, col2 = st.columns(2)
    with col1:
        # Placeholder added here
        retailer_id = st.text_input("Retailer ID", value="", placeholder="e.g. 407")
    with col2:
        # Placeholder added here
        token = st.text_input("Token", value="", type="password", placeholder="Paste token here")
        
    st.divider()
    
    st.header("🗺️ Source Map")
    default_headers = "id, category, secondary_categories, title, brand, link, image_link, price, availability, sale_price, store_id, description"
    headers_input = st.text_area("CSV Header Map (Comma Separated)", value=default_headers, height=150)
    
    st.divider()
    batch_size = st.slider("Batch Size", 10, 100, 50)
    # NEW: Toggle to show deep details
    show_details = st.checkbox("Show Raw Server Responses", value=False)

# --- MAIN INTERFACE ---
uploaded_file = st.file_uploader("📂 Upload Source CSV", type=["csv"])

if uploaded_file:
    user_headers = [h.strip() for h in headers_input.split(',') if h.strip()]
    
    try:
        df = pd.read_csv(uploaded_file, names=user_headers, header=0, encoding='utf-8-sig')
        
        st.write(f"**Preview ({len(df)} rows):**")
        st.dataframe(df.head(3))
        
        if 'id' not in df.columns or 'title' not in df.columns:
            st.error(f"❌ Mapping Error: Columns 'id' and 'title' are required. Found: {df.columns.tolist()}")
            st.stop()

        if st.button("🚀 Start Synchronization", type="primary"):
            if not token:
                st.error("❌ API Token is missing!")
            else:
                progress_bar = st.progress(0)
                status_box = st.empty()
                st.subheader("Transaction Logs")
                
                success_count = 0
                error_count = 0
                records = df.to_dict(orient='records')
                total_records = len(records)
                
                # --- PROCESSING LOOP ---
                for i in range(0, total_records, batch_size):
                    batch = records[i:i+batch_size]
                    
                    # Clean Batch
                    clean_batch = []
                    for row in batch:
                        clean_row = {k: v for k, v in row.items() if pd.notna(v) and str(v).strip() != ""}
                        if clean_row.get('id') and clean_row.get('title'):
                            clean_batch.append(clean_row)
                            
                    if not clean_batch: continue

                    # Send
                    api_headers = {
                        'Content-Type': 'application/json',
                        'x-retailer-id': retailer_id,
                        'x-token': token
                    }
                    payload = {"products": clean_batch}

                    try:
                        resp = requests.post(url, json=payload, headers=api_headers)
                        
                        # --- CAPTURE RESPONSE DETAILS ---
                        try:
                            server_response = resp.json()
                        except:
                            server_response = resp.text
                        
                        batch_num = (i // batch_size) + 1
                        
                        # --- DISPLAY DETAILS ---
                        if resp.status_code == 200:
                            success_count += len(clean_batch)
                            # Create a green expander for success
                            with st.expander(f"✅ Batch {batch_num}: Success ({len(clean_batch)} items)", expanded=show_details):
                                st.write(f"**Status Code:** {resp.status_code}")
                                if show_details:
                                    st.json(server_response)
                        else:
                            error_count += len(clean_batch)
                            # Create a red expander for errors (always show details for errors)
                            with st.expander(f"❌ Batch {batch_num}: Failed (Status {resp.status_code})", expanded=True):
                                st.write("server response:")
                                st.json(server_response)
                            
                    except Exception as e:
                        error_count += len(clean_batch)
                        st.error(f"Network Error: {e}")

                    # Progress
                    progress_bar.progress(min((i + batch_size) / total_records, 1.0))
                    status_box.caption(f"Processing... {min(i + batch_size, total_records)}/{total_records}")
                    time.sleep(0.1)

                st.success(f"Job Complete! Sent: {success_count} | Failed: {error_count}")

    except Exception as e:
        st.error(f"Error reading file: {e}")
