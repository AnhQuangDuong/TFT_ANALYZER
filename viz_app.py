import streamlit as st
import pandas as pd
from pymongo import MongoClient

# Page configuration
st.set_page_config(
    page_title="TFT Meta Analysis",
    page_icon="📊",
    layout="wide"
)

# Title
st.title("TFT Compositions Meta Analysis")

# Connect to MongoDB
@st.cache_resource
def init_connection():
    return MongoClient("mongodb://localhost:27017/")

client = init_connection()

# Fetch data
@st.cache_data(ttl=600)
def get_data():
    db = client["tft_db"]
    collection = db["compositions"]
    # Fetch all documents, excluding the internal _id field
    items = list(collection.find({}, {"_id": 0}))
    return items

try:
    data = get_data()
    
    if data:
        df = pd.DataFrame(data)

        # Formatting for display
        st.subheader(f"Top Compositions (Total: {len(df)})")

        # Display the dataframe with formatting
        st.dataframe(
            df,
            column_config={
                "comp_sig": st.column_config.TextColumn(
                    "Composition Signature",
                    help="Units in the composition",
                    width="medium"
                ),
                "avg_placement": st.column_config.NumberColumn(
                    "Avg Place",
                    format="%.2f",
                ),
                "top4_rate": st.column_config.NumberColumn(
                    "Top 4 %",
                    format="%.1f%%",
                ),
                "pick_rate": st.column_config.NumberColumn(
                    "Pick Rate %",
                    format="%.2f%%",
                ),
                "top_4_carry": st.column_config.ListColumn(
                    "Top Carries",
                    help="Most common carry units",
                )
            },
            hide_index=True,
            width="stretch"
        )
    else:
        st.warning("No data found in the 'tft_db.compositions' collection.")

except Exception as e:
    st.error(f"An error occurred: {e}")
    st.info("Make sure MongoDB is running and the data has been populated.")