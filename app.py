import pandas as pd
import numpy as np
import plotly.graph_objects as go
import streamlit as st
import datetime
import time


CURRENT_DATE = datetime.datetime.now().date()

def display_clock():
    # Create a placeholder for the clock
    clock_placeholder = st.empty()

    while True:
        # Get the current time
        now = datetime.datetime.now()
        current_time = now.strftime("%A %B %d, %Y %I:%M:%S %p")
        
        # Update the placeholder with the current time
        clock_placeholder.metric(label="local time", label_visibility="hidden", value=current_time)

        # Pause for 1 second
        time.sleep(1)

def get_lat_long_from_postal(postal_code):
    """Get latitude and longitude from postal code using Nominatim."""
    try:
        geolocator = Nominatim(user_agent="patient_analysis_dashboard")
        geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
        location = geocode(f"{postal_code}, USA")
        if location:
            return location.latitude, location.longitude
        else:
            return None, None
    except Exception as e:
        print(f"Error geocoding postal code {postal_code}: {str(e)}")
        return None, None

def parse_date_columns(df, date_cols=None, date_format_dict=None):
    """
    Parse date columns in a DataFrame according to specified formats.
    
    Args:
        df (pd.DataFrame): DataFrame containing date columns to parse
        date_cols (list, optional): List of date column names. If None, uses default columns.
        date_format_dict (dict, optional): Dictionary mapping column names to date formats.
                                         If None, uses default format mapping.
    
    Returns:
        pd.DataFrame: DataFrame with parsed date columns
        
    Raises:
        AssertionError: If date parsing fails for any column
    """

    # Default date columns if none provided
    if date_cols is None:
        date_cols = ['date_created']
    
    # Default date format mapping if none provided
    if date_format_dict is None:
        date_format_dict = {
            'date_created': "%m/%d/%Y", 
        }
    
    # Create a copy to avoid modifying original DataFrame
    df_parsed = df.copy()
    
    # Iterate through each date column and parse according to format
    for date_col in date_cols:
        if date_col in df_parsed.columns:
            new_col = df_parsed[date_col].values
            new_col = pd.to_datetime(new_col, format=date_format_dict[date_col])
            df_parsed[date_col] = new_col
    
    # Verify all date columns were parsed successfully
    parsed_date_cols = [col for col in date_cols if col in df_parsed.columns]
    assert (df_parsed[parsed_date_cols].dtypes == np.dtype('<M8[ns]')).all(), \
        "Date parsing failed for one or more columns"
    
    return df_parsed


def get_period(plot_data, prev_plot_data, x):
    if x < plot_data.index[0] and x >= prev_plot_data.index[0]:
        return 'current'
    elif x < prev_plot_data.index[0] and x >= plot_data.index[0]:
        return 'previous'


# write the function for the cell below
def get_current_and_previous_dates(current_end, days):
    """
    A function to get the current and previous dates
    """
    current_start = current_end - datetime.timedelta(days=days)
    previous_start = current_start - datetime.timedelta(days=days)

    return current_end, current_start, previous_start


def get_window_data(df, days):
    """
    A function to get the window data
    """
    
    # join with a time series for month 1, month 2 and month 3
    start_curr = CURRENT_DATE - pd.Timedelta(days=days)
    start_prev = CURRENT_DATE - pd.Timedelta(days=days*2)
    start_prev2 = CURRENT_DATE - pd.Timedelta(days=days*3)
    start_prev3 = CURRENT_DATE - pd.Timedelta(days=days*4)

    ts_curr = pd.Series(pd.date_range(start=start_curr, end=CURRENT_DATE, freq="D"), name='date_created')
    ts_prev = pd.Series(pd.date_range(start=start_prev, end=start_curr, freq="D"), name='date_created')
    ts_prev2 = pd.Series(pd.date_range(start=start_prev2, end=start_prev, freq="D"), name='date_created')
    ts_prev3 = pd.Series(pd.date_range(start=start_prev3, end=start_prev2, freq="D"), name='date_created')
    # get entire data frame of all dates needed for the dashboard
    start_date = start_prev3
    end_date = CURRENT_DATE

    # create filtered data
    raw_data = df[(df['date_created'].dt.date >= start_date) & (df['date_created'].dt.date <= end_date)]
    raw_data = raw_data['date_created'].value_counts().sort_index().reset_index()

    # create a time series for all dates
    ts = pd.DataFrame(pd.date_range(start=start_date, end=end_date, freq="D"), columns=['date_created'])
    data = ts.merge(raw_data, how='outer').fillna(0)
    
    # create current and previous data
    current_data = data[(data['date_created'].dt.date >= start_curr) & (data['date_created'].dt.date < CURRENT_DATE)]
    previous_data = data[(data['date_created'].dt.date >= start_prev) & (data['date_created'].dt.date < start_curr)]
    previous_data2 = data[(data['date_created'].dt.date >= start_prev2) & (data['date_created'].dt.date < start_prev)]
    previous_data3 = data[(data['date_created'].dt.date >= start_prev3) & (data['date_created'].dt.date < start_prev2)]

    # join all data
    current_data = current_data.merge(ts_curr, how='outer').fillna(0)
    previous_data = previous_data.merge(ts_prev, how='outer').fillna(0)
    previous_data2 = previous_data2.merge(ts_prev2, how='outer').fillna(0)
    previous_data3 = previous_data3.merge(ts_prev3, how='outer').fillna(0)
    
    # generate cumsums
    raw_data['cumsum'] = raw_data['count'].cumsum().astype(int)
    data['cumsum'] = data['count'].cumsum().astype(int)
    current_data['cumsum'] = current_data['count'].cumsum().astype(int)
    previous_data['cumsum'] = previous_data['count'].cumsum().astype(int)
    previous_data2['cumsum'] = previous_data2['count'].cumsum().astype(int)
    previous_data3['cumsum'] = previous_data3['count'].cumsum().astype(int)

    return current_data, previous_data, previous_data2, previous_data3


def get_delta_pct(current_data, previous_data):
    """
    A function to get the delta percentage and line colors for the combined data
    """
    # get the delta with combined data
    curr_cumsum_max = current_data['cumsum'].max()
    prev_cumsum_max = previous_data['cumsum'].max() if previous_data['cumsum'].max() != 0 else 1

    delta_pct = (curr_cumsum_max - prev_cumsum_max) / prev_cumsum_max
    # set line color based on delta
    if delta_pct < 0:
        curr_line_color = '#D62728'
    else:
        curr_line_color = '#2CA02C'

    return delta_pct, curr_line_color


def create_combined_data(current_data, previous_data):
    # calc delta pct and set line colors
    delta_pct, curr_line_color = get_delta_pct(current_data, previous_data)

    # --- safety: ensure we have enough rows in previous_data
    if len(previous_data) < 2:
        raise ValueError("previous_data needs at least 2 rows.")

    # --- build the row to prepend (use the second-to-last date from previous_data)
    new_row = pd.DataFrame([{
        'date_created': previous_data.iloc[-2]['date_created'],
        'count': 0,
        'cumsum': 0,
    }], columns=previous_data.columns)

    # --- prepend to current_data (create a fresh copy)
    current_data = pd.concat([new_row, current_data], axis=0, ignore_index=True).copy()

    # --- drop last row from previous_data and take a copy to avoid view-assign warnings
    previous_data = previous_data.iloc[:-1].copy()

    # --- set period labels (safe .loc on real copies)
    previous_data.loc[:, 'period'] = 'previous'
    current_data.loc[:, 'period'] = 'current'

    # --- combine
    combined_data = pd.concat([previous_data, current_data], axis=0, ignore_index=True)

    # --- ensure numeric for cumsum to avoid FutureWarning on object dtypes
    combined_data['count'] = pd.to_numeric(combined_data['count'], errors='coerce')

    # --- compute combined cumsum
    combined_data['combined_cumsum'] = combined_data['count'].fillna(0).cumsum().astype(int)

    return combined_data, delta_pct, curr_line_color


def create_combined_chart(curr, prev, curr_cumsum_max, prev_cumsum_max, days, curr_line_color):
    # instantiate figure in case there is no data
    fig = go.Figure()

    # current period line
    if curr['count'].max() > 0:
        # draw line chart current data
        fig.add_trace(go.Scatter(
            x=curr['date_created'],
            y=curr['combined_cumsum'],
            line_color=curr_line_color,
            # fill='tozeroy',
            mode='lines',
            name=f'Current Period',
            ))
        curr_markers = curr[curr['count'] > 0]
    # previous period line
    if prev['count'].max() > 0:
        # draw line chart previous data
        fig.add_trace(go.Scatter(
        x=prev['date_created'],
        y=prev['combined_cumsum'],
        line_color='lightgrey',
        # fill='tozeroy',
        mode='lines',
        name=f'Previous Period'
        ))

     # add the markers   
    if prev['count'].max() > 0:
        prev_markers = prev[prev['count'] > 0]
            # add markers for prev data
        fig.add_trace(go.Scatter(
            x=prev_markers['date_created'],
            y=prev_markers['combined_cumsum'],
            text=prev_markers['count'],
            line_color='grey',
            mode='markers',
            textposition="top center",
            hovertemplate='<b>Previous Period Marker</b><br>' +
                        'Date: %{x|%a, %b %d %Y}<br>' +
                        'Cumulative Sum: %{y}<br>' +
                        'Count: %{text}<br>' +
                        '<extra></extra>'
        ))

    if curr['count'].max() > 0:
        curr_markers = curr[curr['count'] > 0]
        fig.add_trace(go.Scatter(
            x=curr_markers['date_created'],
            y=curr_markers['combined_cumsum'],
            text=curr_markers['count'],
            line_color=curr_line_color,
            mode='markers',
            textposition="top center",
            hovertemplate='<b>Current Period Marker</b><br>' +
                        'Date: %{x|%a, %b %d %Y}<br>' +
                        'Cumulative Sum: %{y}<br>' +
                        'Count: %{text}<br>' +
                        '<extra></extra>'
        ))


    # title_text = f"({days} day windows) from {prev.iloc[0]["date_created"].strftime("%B %d, %Y")} to {curr.iloc[-1]["date_created"].strftime("%B %d, %Y")}"
    # subtitle_text = f"Current Max: {curr_cumsum_max}, Previous Max: {prev_cumsum_max}"

    # add title
    fig.update_layout(
        height=200,
        width=200,
        showlegend=False,
        # title=dict(text=title_text,
        #     subtitle = dict(text=subtitle_text, 
        #     font=dict(size=12))),
        xaxis_title=None,
        yaxis_title=None,
        margin=dict(
        t=50,  # Top margin
        b=0,
        ),
    )
    # remove grid lines
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=False)
    
    return fig

def generate_streamlit_chart(current_data, previous_data, days):
    # generate first period chart
    combined_data, delta_pct, curr_line_color = create_combined_data(current_data, previous_data)
    curr = combined_data[combined_data['period'] == 'current']
    prev = combined_data[combined_data['period'] == 'previous']
    curr_cumsum_max = combined_data[combined_data['period'] == 'current']['cumsum'].max()
    prev_cumsum_max = combined_data[combined_data['period'] == 'previous']['cumsum'].max()

    fig = create_combined_chart(curr, prev, curr_cumsum_max, prev_cumsum_max, days, curr_line_color)
    st.plotly_chart(fig)

def render_metric_card(data, label: str, value: int, delta_pct: float, period_days: int):
    """
    Render a metric card with consistent styling.
    
    Args:
        label: Metric label
        value: Metric value
        delta: Optional delta/change indicator
        help_text: Optional help text
    """
    
    st.metric(label=label, 
    value=value, 
    delta=f"{delta_pct*100:.1f}%")
    current_data, previous_data = data
    with st.container(border=False):
        generate_streamlit_chart(current_data, previous_data, period_days)

@st.cache_data
def load_data():
    
    file_path = 'data/dates.csv'
    # load data
    df = pd.read_csv(file_path, encoding='utf-8')

    df = df.rename(columns={'DateCreated': 'date_created'})
    # parse date columns
    df = parse_date_columns(df)

    return df

# load data
df = load_data()
st.set_page_config(page_title=None, page_icon="📈", layout='wide', initial_sidebar_state=None, menu_items=None)
# create a container for the header
# create a container for the title and description
header_container = st.container()
header_container.header('Cumulative Onboarding Analysis')
header_container.markdown('Compare current, previous and overall periods')
# Newest patient
# header_container.write(df[df['ClientId'] >= df['ClientId'].max() - 5][['ClientId', 'date_created', 'Name']])
# set base window days
days = header_container.number_input("Select window days", value=30, min_value=1, max_value=90)

st.spinner('Loading data...')
# create window data
current_data, previous_data, previous_data2, previous_data3 = get_window_data(df, days)
# create a container for the metrics
container = st.container()
# set columns
col1, col2, col3 = container.columns(3, border=True)

st.spinner('Loading data...')
with col1:
    combined_data, delta_pct, curr_line_color = create_combined_data(current_data, previous_data)
    st.metric(label="Current v. Last Period", 
        value=f"{current_data['cumsum'].max()} v {previous_data['cumsum'].max()}", 
        delta=f"{delta_pct*100:.1f}%")
    with st.container(border=False):
        generate_streamlit_chart(current_data, previous_data, days)
    
    # with curr_tab:
    #     generate_streamlit_chart(current_data, previous_data, days)

with col2:
    combined_data, delta_pct, curr_line_color = create_combined_data(previous_data, previous_data2)
    st.metric(label="Previous v. Last Period", 
    value=f"{previous_data['cumsum'].max()} v {previous_data2['cumsum'].max()}", 
    delta=f"{delta_pct*100:.1f}%")
    with st.container(border=False):
        generate_streamlit_chart(previous_data, previous_data2, days)
    # with prev_tab: 
    #     generate_streamlit_chart(previous_data, previous_data2, days)

with col3:
    # parse date columns
    with st.container(border=False):
        days *= 2
        current_data, previous_data, previous_data2, previous_data3 = get_window_data(df, days)
        combined_data, delta_pct, curr_line_color = create_combined_data(current_data, previous_data)
        st.metric(label="Overall v. Last Period", 
        value=f"{current_data['cumsum'].max()} v {previous_data['cumsum'].max()}", 
        delta=f"{delta_pct*100:.1f}%")
        generate_streamlit_chart(current_data, previous_data, days)
    # with overall_tab:
    #     generate_streamlit_chart(current_data, previous_data, days)