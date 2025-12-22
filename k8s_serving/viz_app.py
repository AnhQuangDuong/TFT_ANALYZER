import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta
import os

# Page configuration
st.set_page_config(
    page_title="TFT Meta Analysis - Set 16",
    page_icon="📊",
    layout="wide"
)

# Title
st.title("🎮 TFT Meta Analysis - Set 16")
st.markdown("---")

# Connect to MongoDB
@st.cache_resource
def init_connection():
    mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
    return MongoClient(mongodb_uri)

client = init_connection()

# Fetch data from different collections
@st.cache_data(ttl=600)
def get_compositions_data():
    db = client["tft_db"]
    collection = db["compositions"]
    items = list(collection.find({}, {"_id": 0}))
    return items

@st.cache_data(ttl=600)
def get_units_data():
    db = client["tft_db"]
    collection = db["units"]
    items = list(collection.find({}, {"_id": 0}))
    return items

@st.cache_data(ttl=600)
def get_traits_data():
    db = client["tft_db"]
    collection = db["traits"]
    items = list(collection.find({}, {"_id": 0}))
    return items

@st.cache_data(ttl=600)
def get_items_data():
    db = client["tft_db"]
    collection = db["items"]
    items = list(collection.find({}, {"_id": 0}))
    return items

# Batch Layer header and reset button
st.subheader("📊 Batch Layer - Historical Analytics")
col_reset1, col_reset2 = st.columns([6,2])
with col_reset2:
    if st.button("🔄 Refresh Data", key="refresh_batch"):
        st.cache_data.clear()
        st.rerun()

# Create tabs for different views
tab1, tab2, tab3, tab4 = st.tabs(["📋 Compositions", "⚔️ Units", "🎯 Traits", "🛡️ Items"])

# Tab 1: Compositions
with tab1:
    st.header("Top Compositions")
    try:
        data = get_compositions_data()
        
        if data:
            df = pd.DataFrame(data)
            st.metric("Total Compositions", len(df))
            
            st.dataframe(
                df,
                column_config={
                    "comp_sig": st.column_config.TextColumn(
                        "Composition",
                        help="Units in the composition",
                        width="large"
                    ),
                    "avg_placement": st.column_config.NumberColumn(
                        "Avg Place",
                        format="%.2f",
                    ),
                    "top4_rate": st.column_config.NumberColumn(
                        "Top 4 Rate",
                        format="%.2f%%",
                    ),
                    "pick_rate": st.column_config.NumberColumn(
                        "Pick Rate",
                        format="%.2f%%",
                    ),
                    "top_4_carry": st.column_config.ListColumn(
                        "Top Carries",
                        help="Most common carry units",
                    )
                },
                hide_index=True,
                width ='stretch'
            )
        else:
            st.warning("No compositions data found.")
    
    except Exception as e:
        st.error(f"Error loading compositions: {e}")

# Tab 2: Units
with tab2:
    st.header("Unit Tier List")
    try:
        data = get_units_data()
        
        if data:
            df = pd.DataFrame(data)
            
            # Metrics
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.metric("Total Units", len(df))
            with col2:
                st.metric("S Tier", len(df[df['tier'] == 'S']) if 'tier' in df.columns else 0)
            with col3:
                st.metric("A Tier", len(df[df['tier'] == 'A']) if 'tier' in df.columns else 0)
            with col4:
                st.metric("B Tier", len(df[df['tier'] == 'B']) if 'tier' in df.columns else 0)
            with col5:
                st.metric("C Tier", len(df[df['tier'] == 'C']) if 'tier' in df.columns else 0)
            
            st.dataframe(
                df,
                column_config={
                    "unit_id": st.column_config.TextColumn(
                        "Unit",
                        width="medium"
                    ),
                    "tier": st.column_config.TextColumn(
                        "Tier",
                        width="small"
                    ),
                    "avg_place": st.column_config.NumberColumn(
                        "Avg Place",
                        format="%.2f",
                    ),
                    "win_rate": st.column_config.NumberColumn(
                        "Win Rate",
                        format="%.2f%%",
                    ),
                    "frequency": st.column_config.NumberColumn(
                        "Pick Rate",
                        format="%.2f%%",
                    ),
                    "games_with_unit": st.column_config.NumberColumn(
                        "Games",
                        format="%d",
                    ),
                    "popular_items": st.column_config.ListColumn(
                        "Popular Items",
                        help="Most common items on this unit",
                    )
                },
                hide_index=True,
                width ='stretch'
            )
        else:
            st.warning("No units data found.")
    
    except Exception as e:
        st.error(f"Error loading units: {e}")

# Tab 3: Traits
with tab3:
    st.header("Trait Performance")
    try:
        data = get_traits_data()
        
        if data:
            df = pd.DataFrame(data)
            
            # Metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Traits", len(df))
            with col2:
                if 'avg_place' in df.columns:
                    st.metric("Best Avg Place", f"{df['avg_place'].min():.2f}")
            with col3:
                if 'win_rate' in df.columns:
                    st.metric("Highest Win Rate", f"{df['win_rate'].max():.1f}%")
            
            st.dataframe(
                df,
                column_config={
                    "trait_id": st.column_config.TextColumn(
                        "Trait",
                        width="medium"
                    ),
                    "avg_place": st.column_config.NumberColumn(
                        "Avg Place",
                        format="%.2f",
                    ),
                    "win_rate": st.column_config.NumberColumn(
                        "Win Rate",
                        format="%.1f%%",
                    ),
                    "frequency_pct": st.column_config.NumberColumn(
                        "Frequency",
                        format="%.2f%%",
                    ),
                    "avg_tier": st.column_config.NumberColumn(
                        "Avg Tier",
                        format="%.1f",
                    ),
                    "avg_units": st.column_config.NumberColumn(
                        "Avg Units",
                        format="%.1f",
                    ),
                    "popular_tiers": st.column_config.ListColumn(
                        "Popular Tiers",
                        help="Most common tier levels",
                    )
                },
                hide_index=True,
                width ='stretch'
            )
        else:
            st.warning("No traits data found.")
    
    except Exception as e:
        st.error(f"Error loading traits: {e}")

# Tab 4: Items
with tab4:
    st.header("Item Tier List")
    st.markdown("**Place Change**: Negative (better) = Item improves placement | Positive (worse) = Item worsens placement")
    
    try:
        data = get_items_data()
        
        if data:
            df = pd.DataFrame(data)
            
            # Metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Items", len(df))
            with col2:
                if 'avg_place_change' in df.columns:
                    best_impact = df['avg_place_change'].min()
                    st.metric("Best Impact", f"{best_impact:.2f}", 
                             delta=f"{abs(best_impact):.2f} better",
                             delta_color="inverse")
            with col3:
                if 'win_rate' in df.columns:
                    st.metric("Highest Win Rate", f"{df['win_rate'].max():.1f}%")
            
            # Add color coding for place change
            def highlight_place_change(val):
                if pd.isna(val):
                    return ''
                color = 'green' if val < 0 else 'red' if val > 0 else 'black'
                return f'color: {color}; font-weight: bold'
            
            styled_df = df.style.map(
                highlight_place_change, 
                subset=['avg_place_change'] if 'avg_place_change' in df.columns else []
            )
            
            st.dataframe(
                df,
                column_config={
                    "item_name": st.column_config.TextColumn(
                        "Item",
                        width="medium"
                    ),
                    "avg_place": st.column_config.NumberColumn(
                        "Avg Place",
                        format="%.2f",
                    ),
                    "avg_place_change": st.column_config.NumberColumn(
                        "Place Change",
                        format="%.2f",
                        help="Negative = better, Positive = worse"
                    ),
                    "win_rate": st.column_config.NumberColumn(
                        "Win Rate",
                        format="%.1f%%",
                    ),
                    "count": st.column_config.NumberColumn(
                        "Usage Count",
                        format="%d",
                    ),
                    "frequency_pct": st.column_config.NumberColumn(
                        "Frequency",
                        format="%.2f%%",
                    ),
                    "popular_units_cleaned": st.column_config.ListColumn(
                        "Popular On",
                        help="Most common units using this item",
                    )
                },
                hide_index=True,
                width ='stretch'
            )
        else:
            st.warning("No items data found.")
    
    except Exception as e:
        st.error(f"Error loading items: {e}")

# Real-time Speed Layer Section
st.markdown("---")
st.header("⚡ Real-Time Player Analytics (Speed Layer)")
st.markdown("*Live streaming data from recent matches - Updates every 30 seconds*")

# Function to get real-time player data
def get_realtime_players():
    db = client["tft_db"]
    collection = db["players_speed"]
    # Get last 10 players, sorted by timestamp descending
    items = list(collection.find({}, {"_id": 0}).sort("timestamp", -1).limit(15))
    return items

try:
    realtime_data = get_realtime_players()
    
    if realtime_data:
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Players Tracked", len(realtime_data))
        with col2:
            avg_placement = sum(p.get('avg_placement', 0) for p in realtime_data) / len(realtime_data)
            st.metric("Avg Placement", f"{avg_placement:.2f}")
        with col3:
            avg_top4 = sum(p.get('top4_rate', 0) for p in realtime_data) / len(realtime_data)
            st.metric("Avg Top 4 Rate", f"{avg_top4:.1f}%")
        with col4:
            total_games = sum(p.get('total_games', 0) for p in realtime_data)
            st.metric("Total Games", total_games)
        
        # Create DataFrame for display
        players_list = []
        for player in realtime_data:
            # Add 7 hours to timestamp if it exists
            timestamp = player.get('timestamp', 'N/A')
            if timestamp != 'N/A' and isinstance(timestamp, datetime):
                timestamp = timestamp + timedelta(hours=7)
            
            player_info = {
                "Player": f"{player.get('player_name', 'N/A')}#{player.get('player_tag', 'N/A')}",
                "Rank": player.get('player_rank', 'N/A'),
                "Games": player.get('total_games', 0),
                "Avg Place": player.get('avg_placement', 0),
                "Top 4 Rate": player.get('top4_rate', 0),
                "Top 4 Count": player.get('top4_count', 0),
                "1st": player.get('placement_distribution', {}).get('1st', 0),
                "2nd": player.get('placement_distribution', {}).get('2nd', 0),
                "3rd": player.get('placement_distribution', {}).get('3rd', 0),
                "4th": player.get('placement_distribution', {}).get('4th', 0),
                "5th": player.get('placement_distribution', {}).get('5th', 0),
                "6th": player.get('placement_distribution', {}).get('6th', 0),
                "7th": player.get('placement_distribution', {}).get('7th', 0),
                "8th": player.get('placement_distribution', {}).get('8th', 0),
                "Last Update": timestamp
            }
            players_list.append(player_info)
        
        df_realtime = pd.DataFrame(players_list)
        
        # Display table
        st.dataframe(
            df_realtime,
            column_config={
                "Player": st.column_config.TextColumn(
                    "Player",
                    width="large"
                ),
                "Rank": st.column_config.TextColumn(
                    "Rank",
                    width="small"
                ),
                "Games": st.column_config.NumberColumn(
                    "Games",
                    format="%d",
                ),
                "Avg Place": st.column_config.NumberColumn(
                    "Avg Place",
                    format="%.2f",
                ),
                "Top 4 Rate": st.column_config.NumberColumn(
                    "Top 4 %",
                    format="%.1f%%",
                ),
                "Top 4 Count": st.column_config.NumberColumn(
                    "Top 4",
                    format="%d",
                ),
                "1st": st.column_config.NumberColumn("1st", format="%d"),
                "2nd": st.column_config.NumberColumn("2nd", format="%d"),
                "3rd": st.column_config.NumberColumn("3rd", format="%d"),
                "4th": st.column_config.NumberColumn("4th", format="%d"),
                "5th": st.column_config.NumberColumn("5th", format="%d"),
                "6th": st.column_config.NumberColumn("6th", format="%d"),
                "7th": st.column_config.NumberColumn("7th", format="%d"),
                "8th": st.column_config.NumberColumn("8th", format="%d"),
                "Last Update": st.column_config.DatetimeColumn(
                    "Last Update",
                    width="medium"
                )
            },
            hide_index=True,
            width = 'stretch'
        )
        
        # Auto-refresh button
        if st.button("🔄 Refresh Real-time Data"):
            st.rerun()
    
    else:
        st.info("No real-time player data available yet. Waiting for streaming data...")
        if st.button("🔄 Check Again"):
            st.rerun()

except Exception as e:
    st.error(f"Error loading real-time data: {e}")
    import traceback
    st.code(traceback.format_exc())
