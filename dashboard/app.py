# import streamlit as st
# import pandas as pd
# from pymongo import MongoClient

# st.set_page_config(page_title="News Sentiment Dashboard", layout="wide")

# st.title("📰 Real-Time News Sentiment Dashboard")

# client = MongoClient("mongodb://localhost:27017/")
# db = client["newsdb"]
# collection = db["sentiments"]

# data = list(collection.find().sort("_id", -1).limit(50))

# if data:
#     df = pd.DataFrame(data)
#     st.dataframe(df[["title", "sentiment"]])
#     st.bar_chart(df["sentiment"].value_counts())
# else:
#     st.write("No data yet... please wait for news to stream in.")


import streamlit as st
import pandas as pd
from pymongo import MongoClient
import plotly.express as px

# ------------------- PAGE CONFIG -------------------
st.set_page_config(page_title="News Sentiment Dashboard", layout="wide")

st.title("📰 Real-Time News Sentiment Dashboard")

# ------------------- DATABASE CONNECTION -------------------
@st.cache_resource
def get_db_connection():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["newsdb"]
    return db["sentiments"]

collection = get_db_connection()

# ------------------- DATA FETCHING -------------------
def load_data(limit=200):
    data = list(collection.find().sort("_id", -1).limit(limit))
    if data:
        df = pd.DataFrame(data)
        if "_id" in df.columns:
            df.drop(columns=["_id"], inplace=True)
        return df
    return pd.DataFrame()

df = load_data()

# ------------------- FILTERS -------------------
if not df.empty:
    st.sidebar.header("🔍 Filters")

    # Keyword search
    search_keyword = st.sidebar.text_input("Search by keyword")

    # Sentiment filter
    sentiments = df["sentiment"].unique().tolist()
    selected_sentiments = st.sidebar.multiselect("Select Sentiment", sentiments, default=sentiments)

    # Date filter (if timestamp exists)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        min_date, max_date = df["timestamp"].min(), df["timestamp"].max()
        selected_date = st.sidebar.slider("Select Date Range", min_date, max_date, (min_date, max_date))
        df = df[(df["timestamp"] >= selected_date[0]) & (df["timestamp"] <= selected_date[1])]

    # Apply filters
    filtered_df = df[df["sentiment"].isin(selected_sentiments)]
    if search_keyword:
        filtered_df = filtered_df[filtered_df["title"].str.contains(search_keyword, case=False, na=False)]

    # ------------------- DISPLAY DATA -------------------
    st.subheader("📊 Sentiment Distribution")
    sentiment_counts = filtered_df["sentiment"].value_counts().reset_index()
    sentiment_counts.columns = ["sentiment", "count"]

    col1, col2 = st.columns([1, 2])

    with col1:
        st.write("### Sentiment Count")
        st.bar_chart(sentiment_counts.set_index("sentiment"))

    with col2:
        fig = px.pie(sentiment_counts, values="count", names="sentiment", title="Sentiment Share", color_discrete_sequence=px.colors.qualitative.Set2)
        st.plotly_chart(fig, use_container_width=True)

    st.write("### 🧾 News Feed")
    st.dataframe(filtered_df[["title", "sentiment"]])

else:
    st.warning("⚠️ No data found. Please wait for news to stream in or check your database connection.")

