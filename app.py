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
        retailer_id = st.text_input("Retailer ID", value="", placeholder="Add Agency ID")
    with col2:
        token = st.text_input("Token", value="", type="password", placeholder="Paste token here")
        
    st.divider()
    
    st.header("🗺️ Source Map")
    st.info("Define how your CSV columns map to the API fields. The order must match your CSV columns.")
    
    # --- RESTORED FEATURE: HEADER MAPPING ---
    default_headers = "id, category, secondary_categories, title, brand, link, image_link, price, availability, sale_price, store_id, description"
    headers_input = st.text_area("CSV Header Map (Comma Separated)", value=default_headers, height=150)
    
    st.divider()
    batch_size = st.slider("Batch Size", 10, 100, 50)

# --- MAIN INTERFACE ---
uploaded_file = st.file_uploader("📂 Upload Source CSV", type=["csv"])

if uploaded_file:
    # 1. Parse the User's Header Map
    # Split string by comma and remove spaces
    user_headers = [h.strip() for h in headers_input.split(',') if h.strip()]
    
    try:
        # 2. Read CSV using the User's Headers
        # header=0 means "The file has a header row, but ignore it and use my 'names' list instead"
        df = pd.read_csv(uploaded_file, names=user_headers, header=0, encoding='utf-8-sig')
        
        st.write(f"**Preview ({len(df)} rows):**")
        st.dataframe(df.head(3))
        
        # Validation
        if 'id' not in df.columns or 'title' not in df.columns:
            st.error(f"❌ Mapping Error: The app expects columns named 'id' and 'title'. Your map has: {df.columns.tolist()}")
            st.stop()

        # --- ACTION ---
        if st.button("🚀 Start Synchronization", type="primary"):
            if not token:
                st.error("❌ API Token is missing!")
            else:
                progress_bar = st.progress(0)
                logs_expander = st.expander("View Logs", expanded=True)
                
                success_count = 0
                error_count = 0
                records = df.to_dict(orient='records')
                total_records = len(records)
                
                # --- LOOP ---
                for i in range(0, total_records, batch_size):
                    batch = records[i:i+batch_size]
                    
                    # Clean Batch
                    clean_batch = []
                    for row in batch:
                        # Remove empty keys/values
                        clean_row = {k: v for k, v in row.items() if pd.notna(v) and str(v).strip() != ""}
                        
                        # Basic validation
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
                        
                        if resp.status_code == 200:
                            success_count += len(clean_batch)
                            logs_expander.success(f"Batch {i//batch_size + 1}: OK ({len(clean_batch)} items)")
                        else:
                            error_count += len(clean_batch)
                            logs_expander.error(f"Batch {i//batch_size + 1}: Failed ({resp.status_code})")
                            
                    except Exception as e:
                        error_count += len(clean_batch)
                        logs_expander.error(f"Network Error: {e}")

                    # Progress
                    progress_bar.progress(min((i + batch_size) / total_records, 1.0))
                    time.sleep(0.1)

                st.success(f"Job Complete! Sent: {success_count} | Failed: {error_count}")

    except Exception as e:
        st.error(f"Error reading file: {e}")
        st.warning("Tip: Make sure the number of items in your 'Header Map' matches the number of columns in your CSV.")


