import os
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from pymongo import MongoClient


load_dotenv()

st.set_page_config(page_title="TFT Unit Tier List", page_icon="📊", layout="wide")
st.title("TFT Unit Tier List")


@st.cache_data(show_spinner=False)
def load_from_mongo(uri: str, db: str, collection: str, limit: int = 5000):
    client = MongoClient(uri, serverSelectionTimeoutMS=2000)
    coll = client[db][collection]
    docs = list(coll.find({}, {"_id": 0}).limit(limit))
    return pd.DataFrame(docs)


png_path = Path(__file__).parent / "tft_unit_tier_list.png"

st.write(
    "Chon nguon hien thi: doc tu MongoDB (collection unit_stats) hoac xem anh tier "
    "da render san tu script unit_tier_list.py."
)

with st.expander("Doc du lieu tu MongoDB", expanded=True):
    default_uri = os.getenv(
        "MONGO_URI",
        "mongodb://localhost:27017",
    )
    uri = st.text_input("Mongo URI", value=default_uri)
    db_name = st.text_input("Database", value="tft_db")
    col_name = st.text_input("Collection", value="unit_stats")
    limit = st.number_input("Gioi han ban ghi", min_value=10, max_value=50000, value=5000, step=100)

    if st.button("Tai du lieu"):
        try:
            df = load_from_mongo(uri, db_name, col_name, limit)
            if df.empty:
                st.warning("Khong co du lieu (collection trong hoac khong co ban ghi).")
            else:
                st.success(f"Tai {len(df)} ban ghi tu MongoDB.")
                st.dataframe(df, use_container_width=True, hide_index=True)
        except Exception as exc:  # pylint: disable=broad-except
            st.error(f"Loi khi doc MongoDB: {exc}")

st.divider()

st.subheader("Anh tier list co san")
if png_path.exists():
    st.image(str(png_path), caption="TFT Unit Tier List", use_container_width=True)
else:
    st.warning(
        "Chua thay file `tft_unit_tier_list.png`. "
        "Chay: `python unit_tier_list.py` (kem bien moi truong PYSPARK_SUBMIT_ARGS nhu huong dan) de tao anh."
    )
