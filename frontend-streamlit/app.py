import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.graph_objects as go

# --- Data Loading Functions ---

@st.cache_data
def get_data_from_snowflake(table_name: str):
    """
    Connects to Snowflake and fetches data from a specified table in the GOLD schema.
    """
    try:
        with snowflake.connector.connect(**st.secrets["snowflake"]) as conn:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, conn)
            for col in df.columns:
                if 'TIME' in col or 'DATE' in col:
                    df[col] = pd.to_datetime(df[col])
            return df
    except snowflake.connector.errors.ProgrammingError as e:
        st.error(f"Snowflake Error: {e}")
        st.error(f"Please check if the table '{table_name}' exists in the GOLD schema.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return pd.DataFrame()

# --- Streamlit App Layout ---

st.set_page_config(layout="wide")
st.title("📈 Advanced Stock Analysis Dashboard")
st.markdown("Visualizing data from the dbt Gold layer.")

# Load data from the Gold models
kpi_df = get_data_from_snowflake("gold_kpi")
candlestick_chart_df = get_data_from_snowflake("gold_treechart") # This model has candlestick data
volatility_df = get_data_from_snowflake("gold_candlestick") # This model has volatility data

if kpi_df.empty or candlestick_chart_df.empty or volatility_df.empty:
    st.warning("Could not load one or more data tables. Please check your Snowflake connection and ensure all Gold models exist.")
else:
    # --- Interactive Sidebar ---
    st.sidebar.header("Filters")
    symbols = sorted(kpi_df['SYMBOL'].unique())
    selected_symbol = st.sidebar.selectbox("Select a Stock Symbol", symbols)

    # --- Main Page with Tabs ---
    tab1, tab2 = st.tabs(["Stock Overview", "Market Comparison"])

    with tab1:
        # --- Stock Overview for a selected symbol ---
        st.header(f"Analysis for: {selected_symbol}")

        # Filter data based on selection
        selected_kpi = kpi_df[kpi_df['SYMBOL'] == selected_symbol].iloc[0]
        selected_candlestick_data = candlestick_chart_df[candlestick_chart_df['SYMBOL'] == selected_symbol].copy().sort_values('CANDLE_TIME')

        # Display KPI metrics
        st.subheader("Latest Performance")
        current_price = selected_kpi['CURRENT_PRICE']
        change_amount = selected_kpi['CHANGE_AMOUNT']
        change_percent = selected_kpi['CHANGE_PERCENT']

        col1, col2 = st.columns(2)
        col1.metric("Current Price", f"${current_price:,.2f}")
        delta_color = "inverse" if change_amount < 0 else "normal"
        col2.metric("Change", f"${change_amount:,.2f}", f"{change_percent:,.2f}%", delta_color=delta_color)

        # --- Candlestick Chart ---
        st.subheader("Historical Price (Last 12 Days)")

        fig_candle = go.Figure()
        fig_candle.add_trace(go.Candlestick(
            x=selected_candlestick_data['CANDLE_TIME'],
            open=selected_candlestick_data['CANDLE_OPEN'],
            high=selected_candlestick_data['CANDLE_HIGH'],
            low=selected_candlestick_data['CANDLE_LOW'],
            close=selected_candlestick_data['CANDLE_CLOSE'],
            name='Price'
        ))
        # Add trendline from your model
        fig_candle.add_trace(go.Scatter(
            x=selected_candlestick_data['CANDLE_TIME'],
            y=selected_candlestick_data['TREND_LINE'],
            mode='lines',
            name='Trend Line',
            line=dict(color='orange', width=2, dash='dot')
        ))

        fig_candle.update_layout(
            xaxis_rangeslider_visible=False,
            height=600,
            title_text=f'{selected_symbol} Candlestick Chart',
            yaxis_title='Price (USD)',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig_candle, use_container_width=True)

        with st.expander("View Raw Candlestick Data"):
            st.dataframe(selected_candlestick_data)

    with tab2:
        # --- Market Comparison Tab ---
        st.header("Market-Wide Comparison")
        st.markdown("Comparing all stocks based on their recent volatility.")

        # Sort by volatility for better visualization
        sorted_volatility_df = volatility_df.sort_values('VOLATILITY', ascending=False)

        # --- Volatility Bar Chart ---
        st.subheader("Stock Volatility (Standard Deviation)")
        fig_vol = go.Figure(go.Bar(
            x=sorted_volatility_df['SYMBOL'],
            y=sorted_volatility_df['VOLATILITY'],
            text=sorted_volatility_df['VOLATILITY'].apply(lambda x: f'${x:,.2f}'),
            textposition='auto',
            marker_color='rgb(26, 118, 255)'
        ))
        fig_vol.update_layout(
            height=500,
            xaxis_title="Stock Symbol",
            yaxis_title="Volatility (Std. Dev. of Price)",
            xaxis={'categoryorder':'total descending'}
        )
        st.plotly_chart(fig_vol, use_container_width=True)
        
        # --- Relative Volatility Bar Chart ---
        st.subheader("Relative Volatility (Volatility / Average Price)")
        sorted_rel_vol_df = volatility_df.sort_values('RELATIVE_VOLATILITY', ascending=False)
        
        fig_rel_vol = go.Figure(go.Bar(
            x=sorted_rel_vol_df['SYMBOL'],
            y=sorted_rel_vol_df['RELATIVE_VOLATILITY'],
            text=sorted_rel_vol_df['RELATIVE_VOLATILITY'].apply(lambda x: f'{x:.2%}'),
            textposition='auto',
            marker_color='rgb(55, 83, 109)'
        ))
        fig_rel_vol.update_layout(
            height=500,
            xaxis_title="Stock Symbol",
            yaxis_title="Relative Volatility",
            yaxis_tickformat=".0%",
            xaxis={'categoryorder':'total descending'}
        )
        st.plotly_chart(fig_rel_vol, use_container_width=True)
        
        with st.expander("View Raw Volatility Data"):
            st.dataframe(volatility_df)
