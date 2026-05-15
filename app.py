import streamlit as st
import pandas as pd
import requests
import time
import json

# --- PAGE SETUP ---
st.set_page_config(page_title="Data Sync Pro", page_icon="⏱️", layout="centered")

st.title("⏱️ Data Sync Pro")
st.markdown("Syncing CSV data with forced String types and CURL debugging.")
st.divider()

# --- SIDEBAR CONFIGURATION ---
with st.sidebar:
    st.header("⚙️ Connection Settings")
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
    st.header("⚡ Performance Control")
    req_per_sec = st.slider("Max Requests per Second", 1, 10, 5, help="Controls the speed to avoid hitting API limits.")
    batch_size = st.slider("Batch Size", 10, 100, 50)
    
    st.divider()
    st.header("🗺️ Source Map")
    # Mapped based on your specific CSV file columns
    default_headers = "dealer_code, city, state, id, brand_id, category, image_link, link, description, title, price, availability"
    headers_input = st.text_area("CSV Header Map", value=default_headers, height=150)
    
    show_details = st.checkbox("Show Raw Server Responses", value=True)

# --- MAIN INTERFACE ---
uploaded_file = st.file_uploader("📂 Upload Source CSV", type=["csv"])

if uploaded_file:
    user_headers = [h.strip() for h in headers_input.split(',') if h.strip()]
    
    try:
        # CRITICAL: dtype=str ensures brand_id and dealer_code are strings, not numbers
        df = pd.read_csv(
            uploaded_file, 
            names=user_headers, 
            header=0, 
            encoding='utf-8-sig',
            dtype=str
        )
        
        # Replace NaN values with empty strings to keep JSON clean
        df = df.fillna("")
        
        st.write(f"**Preview ({len(df)} rows):**")
        st.dataframe(df.head(3))
        
        if st.button("🚀 Start Synchronization", type="primary"):
            if not token:
                st.error("❌ API Token is missing!")
            else:
                # --- CURL GENERATOR FOR DEBUGGING ---
                # We generate the CURL for the first batch to verify the payload format
                first_batch = df.head(batch_size).to_dict(orient='records')
                sample_payload = {"products": first_batch}
                
                curl_command = f"""curl --location '{url}' \\
--header 'Content-Type: application/json' \\
--header 'x-retailer-id: {retailer_id}' \\
--header 'x-token: {token}' \\
--data '{json.dumps(sample_payload)}'"""

                st.subheader("🛠️ Debug: First Batch CURL")
                st.info("Copy this to your terminal to test the API manually:")
                st.code(curl_command, language="bash")
                
                st.divider()
                
                # --- PROCESSING LOOP ---
                progress_bar = st.progress(0)
                status_box = st.empty()
                st.subheader("Transaction Logs")
                
                success_count = 0
                error_count = 0
                records = df.to_dict(orient='records')
                total_records = len(records)
                sleep_time = 1.0 / req_per_sec
                
                for i in range(0, total_records, batch_size):
                    batch = records[i:i+batch_size]
                    
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
                            with st.expander(f"✅ Batch {batch_num}: Success ({len(batch)} items)", expanded=False):
                                st.write(f"**Status:** {resp.status_code}")
                                if show_details:
                                    st.json(server_response)
                        else:
                            error_count += len(batch)
                            with st.expander(f"❌ Batch {batch_num}: Failed (Status {resp.status_code})", expanded=True):
                                st.write("**Request Payload Sample (First Item):**")
                                st.json(batch[0]) # Show the first item to verify brand_id is a string
                                st.write("**Server Response:**")
                                st.json(server_response)
                            
                    except Exception as e:
                        error_count += len(batch)
                        st.error(f"Network Error on Batch {i//batch_size + 1}: {e}")

                    # Progress Update
                    current_progress = min((i + batch_size) / total_records, 1.0)
                    progress_bar.progress(current_progress)
                    status_box.caption(f"Processing... {min(i + batch_size, total_records)}/{total_records}")
                    
                    # Rate limiting sleep
                    time.sleep(sleep_time) 

                st.success(f"Job Complete! Sent: {success_count} | Failed: {error_count}")

    except Exception as e:
        st.error(f"Error reading file: {e}")
