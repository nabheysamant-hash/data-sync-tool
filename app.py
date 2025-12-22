import streamlit as st
import pandas as pd
import requests
import time
import json

# --- PAGE SETUP ---
st.set_page_config(page_title="Data Sync Pro", page_icon="🔁", layout="centered")

st.title("🔁 Data Sync Pro v2")
st.markdown("Sync your CSV data to the **OnlineSales.ai Catalog Sync Service**.")
st.divider()

# --- SIDEBAR CONFIGURATION ---
with st.sidebar:
    st.header("⚙️ Connection Settings")
    
    # 1. UPDATED ENDPOINT URL
    url = st.text_input(
        "Endpoint URL", 
        value="https://apiv2.onlinesales.ai/catalogSyncService/products"
    )
    
    col1, col2 = st.columns(2)
    with col1:
        retailer_id = st.text_input("Retailer ID", value="", placeholder="e.g. 407")
    with col2:
        token = st.text_input("Token", value="", type="password", placeholder="Paste token here")
        
    st.divider()
    
    st.header("🗺️ Source Map")
    default_headers = "id, category, secondary_categories, title, brand, link, image_link, price, availability, sale_price, store_id, description"
    headers_input = st.text_area("CSV Header Map (Comma Separated)", value=default_headers, height=150)
    
    st.divider()
    batch_size = st.slider("Batch Size", 10, 100, 50)
    # Checkbox to auto-expand details
    auto_expand = st.checkbox("Auto-expand Log Details", value=False)

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

                    # Prepare Request
                    api_headers = {
                        'Content-Type': 'application/json',
                        'x-retailer-id': retailer_id,
                        'x-token': token
                    }
                    payload = {"products": clean_batch}

                    try:
                        # Send Request
                        resp = requests.post(url, json=payload, headers=api_headers)
                        
                        # --- 2. DETAILED RESPONSE CAPTURE ---
                        try:
                            # Try to parse JSON response
                            response_body = resp.json()
                        except:
                            # If not JSON, capture raw text (e.g., HTML error page)
                            response_body = resp.text

                        batch_num = (i // batch_size) + 1
                        is_success = resp.status_code == 200
                        
                        # Icon and Label
                        icon = "✅" if is_success else "❌"
                        label = f"Batch {batch_num}: {resp.status_code} ({len(clean_batch)} items)"
                        
                        # Update Counters
                        if is_success:
                            success_count += len(clean_batch)
                        else:
                            error_count += len(clean_batch)

                        # --- LOG DISPLAY ---
                        # We use an expander to hold the details
                        with st.expander(f"{icon} {label}", expanded=auto_expand or not is_success):
                            
                            # Use Tabs to organize the detailed view
                            tab1, tab2, tab3 = st.tabs(["Response Body", "Sent Payload", "Response Headers"])
                            
                            with tab1:
                                st.markdown("#### Server Response")
                                st.json(response_body)
                                
                            with tab2:
                                st.markdown("#### What we sent")
                                st.json(payload)
                                
                            with tab3:
                                st.markdown("#### Meta Data")
                                st.write(f"**Status Code:** {resp.status_code}")
                                st.write("**Headers:**")
                                st.write(dict(resp.headers))

                    except Exception as e:
                        error_count += len(clean_batch)
                        st.error(f"Network Error: {e}")

                    # Progress Bar Update
                    progress_bar.progress(min((i + batch_size) / total_records, 1.0))
                    status_box.caption(f"Processing... {min(i + batch_size, total_records)}/{total_records}")
                    time.sleep(0.1)

                st.success(f"Job Complete! Sent: {success_count} | Failed: {error_count}")

    except Exception as e:
        st.error(f"Error reading file: {e}")
