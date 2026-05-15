import streamlit as st
import pandas as pd
import requests
import time
import json

# --- PAGE SETUP ---
st.set_page_config(page_title="Data Sync Pro (No Validation)", page_icon="⏱️", layout="centered")

st.title("⏱️ Data Sync Pro (Unvalidated)")
st.markdown("Sync CSV data directly to API without pre-flight checks.")
st.divider()

# --- SIDEBAR CONFIGURATION ---
with st.sidebar:
    st.header("⚙️ Connection Settings")
    url = st.text_input("Endpoint URL", value="https://apiv2.onlinesales.ai/catalogSyncService/products")
    
    col1, col2 = st.columns(2)
    with col1:
        retailer_id = st.text_input("Retailer ID", value="", placeholder="e.g. 407")
    with col2:
        token = st.text_input("Token", value="", type="password", placeholder="Paste token here")
        
    st.divider()
    st.header("⚡ Performance Control")
    req_per_sec = st.slider("Max Requests per Second", 1, 10, 5)
    batch_size = st.slider("Batch Size", 10, 100, 50)
    
    st.divider()
    st.header("🗺️ Source Map")
    default_headers = "id, category, secondary_categories, title, brand, link, image_link, price, availability, sale_price, store_id, description"
    headers_input = st.text_area("CSV Header Map", value=default_headers, height=150)
    
    show_details = st.checkbox("Show Raw Server Responses", value=True)

# --- MAIN INTERFACE ---
uploaded_file = st.file_uploader("📂 Upload Source CSV", type=["csv"])

if uploaded_file:
    user_headers = [h.strip() for h in headers_input.split(',') if h.strip()]
    
    try:
        # Load data without strict requirements
        df = pd.read_csv(uploaded_file, names=user_headers, header=0, encoding='utf-8-sig')
        st.write(f"**Preview ({len(df)} rows):**")
        st.dataframe(df.head(3))
        
        if st.button("🚀 Start Synchronization", type="primary"):
            if not token:
                st.error("❌ API Token is missing!")
            else:
                progress_bar = st.progress(0)
                status_box = st.empty()
                st.subheader("Transaction Logs")
                
                success_count = 0
                error_count = 0
                # Convert entire dataframe to records, including NaNs/empty values
                records = df.to_dict(orient='records')
                total_records = len(records)
                sleep_time = 1.0 / req_per_sec
                
                for i in range(0, total_records, batch_size):
                    batch = records[i:i+batch_size]
                    
                    # API Headers
                    api_headers = {
                        'Content-Type': 'application/json',
                        'x-retailer-id': retailer_id,
                        'x-token': token
                    }
                    payload = {"products": batch}

                    try:
                        resp = requests.post(url, json=payload, headers=api_headers)
                        
                        try:
                            server_response = resp.json()
                        except:
                            server_response = resp.text
                        
                        batch_num = (i // batch_size) + 1
                        
                        if resp.status_code == 200:
                            success_count += len(batch)
                            with st.expander(f"✅ Batch {batch_num}: Sent ({len(batch)} items)", expanded=show_details):
                                st.json(server_response)
                        else:
                            error_count += len(batch)
                            with st.expander(f"❌ Batch {batch_num}: Failed (Status {resp.status_code})", expanded=True):
                                st.write("Server response:")
                                st.json(server_response)
                            
                    except Exception as e:
                        error_count += len(batch)
                        st.error(f"Network Error: {e}")

                    # Progress Update
                    progress_bar.progress(min((i + batch_size) / total_records, 1.0))
                    status_box.caption(f"Processing... {min(i + batch_size, total_records)}/{total_records}")
                    
                    time.sleep(sleep_time) 

                st.success(f"Job Complete! Processed: {success_count + error_count} | Errors: {error_count}")

    except Exception as e:
        st.error(f"Error reading file: {e}")
