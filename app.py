import pandas as pd
import numpy as np
import plotly.graph_objects as go
import streamlit as st
import datetime
from zoneinfo import ZoneInfo
import time
from millify import millify
from PIL import Image
from refresh_data_in_app import refresh_data_in_app
from run_data_pipeline import run_data_pipeline

CURRENT_DATE = datetime.datetime.now(ZoneInfo("America/Los_Angeles")).date()
API_KEY = st.secrets["INTAKEQ_API_KEY"]

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

    # create current period
    current_data = ts.merge(raw_data, how='outer').fillna(0)
    current_data = current_data[(current_data['date_created'].dt.date > start_curr) & (current_data['date_created'].dt.date <= CURRENT_DATE)]

    # create previous period
    previous_data = ts.merge(raw_data, how='outer').fillna(0)
    previous_data = previous_data[(previous_data['date_created'].dt.date > start_prev) & (previous_data['date_created'].dt.date <= start_curr)]

    # create prior previous period
    previous_data2 = ts.merge(raw_data, how='outer').fillna(0)
    previous_data2 = previous_data2[(previous_data2['date_created'].dt.date > start_prev2) & (previous_data2['date_created'].dt.date <= start_prev)]

    previous_data3 = ts.merge(raw_data, how='outer').fillna(0)
    previous_data3 = previous_data3[(previous_data3['date_created'].dt.date > start_prev3) & (previous_data3['date_created'].dt.date <= start_prev2)]

    # join all data
    current_data = current_data.merge(ts_curr, how='outer').fillna(0)
    previous_data = previous_data.merge(ts_prev, how='outer').fillna(0)
    previous_data2 = previous_data2.merge(ts_prev2, how='outer').fillna(0)
    
    # generate cumsums
    data['cumsum'] = data['count'].cumsum().astype(int)
    current_data['cumsum'] = current_data['count'].cumsum().astype(int)
    previous_data['cumsum'] = previous_data['count'].cumsum().astype(int)
    previous_data2['cumsum'] = previous_data2['count'].cumsum().astype(int)
    previous_data3['cumsum'] = previous_data3['count'].cumsum().astype(int)

    return data, current_data, previous_data, previous_data2, previous_data3

def calc_delta(curr, prev):
    if prev == 0:
        # handle edge case: no valid baseline
        if curr == 0:
            return 0.0
        else:
            return 1.0  # or 100.0 if you want capped growth
    return (((curr - prev) / prev))

def get_delta_pct(current_data, previous_data):
    """
    A function to get the delta percentage and line colors for the combined data
    """
    # get the delta with combined data
    curr_cumsum_max = current_data['cumsum'].max()
    prev_cumsum_max = previous_data['cumsum'].max()

    delta_pct = calc_delta(curr_cumsum_max, prev_cumsum_max)

    # set line color based on delta
    # dark_theme = darkdetect.isDark()
    # light_theme = darkdetect.isLight()

    # red for negative delta
    if delta_pct < 0:
        # if light_theme: 
        curr_line_color_bg = 'rgb(255, 43, 43)'
        curr_line_color = 'rgba(255, 43, 43, 0.1)'
        # elif dark_theme:
        #     curr_line_color_bg = 'rgb(255, 75, 75)'
        #     curr_line_color = 'rgba(255, 108, 108, 0.3)'
        # curr_line_color = '#ff2b2b'
    
    # green for positive delta
    else:
        # if light_theme:
        curr_line_color_bg = 'rgb(21, 130, 55)'
        curr_line_color = 'rgba(33, 195, 84, 0.1)'
        # elif dark_theme:
        # curr_line_color_bg = 'rgb(61, 213, 109)'
        # curr_line_color = 'rgba(61, 213, 109, 0.3)'
        # curr_line_color = '#2CA02C'

    return delta_pct, curr_line_color, curr_line_color_bg

def create_combined_data(current_data, previous_data):
   
    delta_pct, curr_line_color, curr_line_color_bg = get_delta_pct(current_data, previous_data)
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

    return combined_data, delta_pct, curr_line_color, curr_line_color_bg


def create_combined_chart(curr, prev, curr_line_color, curr_line_color_bg, markers=True, lines=True):
    # instantiate figure in case there is no data
    fig = go.Figure()
    marker_size = 4

    # current period line
    if curr['count'].max() > 0 and lines:
        # draw line chart current data
        fig.add_trace(go.Scatter(
            x=curr['date_created'],
            y=curr['cumsum'],
            fill='tonexty',
            fillcolor=curr_line_color,
            text=curr['count'],
            line_color=curr_line_color_bg,
            # fill='tozeroy',
            mode='lines',
            name=f'Current Period',
            hoverlabel=dict(
                # bgcolor= curr_line_color # Set background color for all hover labels (if not overridden by trace)
                bordercolor=curr_line_color_bg
            ),
            ))
        curr_markers = curr[curr['count'] > 0]

    # previous period line
    if prev['count'].max() > 0 and lines:
        # draw line chart previous data
        fig.add_trace(go.Scatter(
        x=prev['date_created'],
        y=prev['cumsum'],
        fill='tozeroy',
        text=prev['count'],
        line_color='lightgrey',
        # fill='tozeroy',
        mode='lines',
        name=f'Previous Period',
        hoverlabel=dict(
                # bgcolor= curr_line_color # Set background color for all hover labels (if not overridden by trace)
                bordercolor='grey'
            ),
        ))
        prev_markers = prev[prev['count'] > 0]

     # add the prev markers   
    if prev['count'].max() > 0 and markers:
        # add markers for prev data
        fig.add_trace(go.Scatter(
            x=prev_markers['date_created'],
            y=prev_markers['cumsum'],
            text=prev_markers['count'],
            line_color=None,
            mode='markers',
            # marker_symbol=105,
            marker=dict(size=marker_size, color='white', line=dict(width=1,
                                        color='grey',
                                        )),
            textposition="top right",
            hoverlabel=dict(
                # bgcolor= curr_line_color # Set background color for all hover labels (if not overridden by trace)
                bordercolor='grey'
            ), 
        ))
    
    # Curr markers
    if curr['count'].max() > 0 and markers:
        curr_markers = curr[curr['count'] > 0]
        fig.add_trace(go.Scatter(
            x=curr_markers['date_created'],
            y=curr_markers['cumsum'],
            text=curr_markers['count'],
            line_color=None,
            mode='markers',
            # marker_symbol=105,
            marker=dict(size=marker_size, color='white', 
                        line=dict(width=1,
                        color='grey',
                        )),
            textposition="bottom left",
            hoverlabel=dict(
                # bgcolor= curr_line_color # Set background color for all hover labels (if not overridden by trace)
                bordercolor=curr_line_color_bg
            ),   
        ))

    # title_text = f"({days} day windows) from {prev.iloc[0]["date_created"].strftime("%B %d, %Y")} to {curr.iloc[-1]["date_created"].strftime("%B %d, %Y")}"
    # subtitle_text = f"Current Max: {curr_cumsum_max}, Previous Max: {prev_cumsum_max}"

    # add hover template
    fig.update_traces(
        hovertemplate=
            'Period Total: <b>%{y}</b><br>' +
            'Onboarded: %{text}<br>' +
            "%{x|%B %d, %Y}</b>" +
            "<extra></extra>"
    )

    # remove tick labels
    fig.update_xaxes(showgrid=False, tickangle=325, tickformat="%b %d, %Y")
    fig.update_yaxes(showgrid=False) # Hides y-axis tick labels

     # add title
    fig.update_layout(
        # title=dict(text=str(prev['cumsum'].max())),
        height=200,
        width=200,
        showlegend=False,
        # title=dict(text=title_text,
        #     subtitle = dict(text=subtitle_text, 
        #     font=dict(size=12))),
        xaxis_title=None,
        yaxis_title=None,
        xaxis=dict(fixedrange=True),
        yaxis=dict(fixedrange=True, showticklabels=False),
        margin=dict(
        t=25,  # Top margin
        b=0,
        ),
    )
    return fig

def generate_streamlit_chart(combined_data, show_markers, chart_id):
    # generate first period chart
    curr = combined_data[combined_data['period'] == 'current']
    prev = combined_data[combined_data['period'] == 'previous']

    fig = create_combined_chart(curr, prev, curr_line_color, curr_line_color_bg, show_markers, lines=True)
    # with st.expander("Previous Total", icon='↩️', expanded=True):
    st.plotly_chart(fig, use_container_width=False, key=chart_id, height=200, config={'modeBarButtonsToRemove': [
                'zoomIn2d', 'zoomOut2d', 'zoom',
                'pan2d', 'select2d', 'lasso2d',
                'autoScale2d', 'resetScale2d',
                'hoverClosestCartesian', 'hoverCompareCartesian',
                'toImage']
                }
            )
    
    # with st.expander("combined chart", icon='📉'):
    #     line_chart = st.line_chart(prev,
    #                                height=200, 
    #                                x='date_created', 
    #                                y=['combined_cumsum'],
    #                                x_label='',
    #                                y_label='', 
    #                                color='period')
    #     line_chart.add_rows(curr)

def load_data(run_live=False):
    clients, appts = None, None
    with st.spinner("Refreshing data..."):
        if run_live:
            clients, appts = refresh_data_in_app(API_KEY)
    with st.spinner("Running data pipeline..."):
        if run_live:
                clients, appts = run_data_pipeline(clients, appts)
        else:   
            # load data
            clients = pd.read_csv('data/dates.csv', encoding='utf-8')
            appts = pd.read_csv('data/appt_dates.csv', encoding='utf-8')
        
        clients = clients.rename(columns={'DateCreated': 'date_created'})
        # parse date columns
        clients = parse_date_columns(clients)
        # convert datetime
        appts['Date'] = pd.to_datetime(appts['Date'])
        appts['CancellationDate'] = pd.to_datetime(appts['CancellationDate'], errors='coerce')

        return clients, appts

def filter_data(practitioner_appts, days):
    now = CURRENT_DATE
    # filter appointments
    practitioner_appts = practitioner_appts[(practitioner_appts['CancellationDate'].isna()) & (practitioner_appts['Status'] == 'Confirmed')]
    appts_this_year = practitioner_appts[practitioner_appts['Date'].dt.date >= now - pd.Timedelta(days=days)]
    appts_last_year = practitioner_appts[(practitioner_appts['Date'].dt.date >= now - pd.Timedelta(days=days*2)) & 
                                         (practitioner_appts['Date'].dt.date < now - pd.Timedelta(days=days))]
    # delta_appts_pct = calc_delta(len(appts_this_year), len(appts_last_year))

    new_appts_this_year = appts_this_year[~(appts_this_year['ServiceId'] == '1efe2465-4741-48c8-8408-114818cdce74')]
    new_appts_last_year = appts_last_year[~(appts_last_year['ServiceId'] == '1efe2465-4741-48c8-8408-114818cdce74')]

    follow_up_appts_this_year = appts_this_year[appts_this_year['ServiceId'] == '1efe2465-4741-48c8-8408-114818cdce74']
    follow_up_appts_last_year = appts_last_year[appts_last_year['ServiceId'] == '1efe2465-4741-48c8-8408-114818cdce74']

    total_paid_this_year = appts_this_year['Price'].sum()
    total_paid_last_year = appts_last_year['Price'].sum()

    return (new_appts_this_year, new_appts_last_year, follow_up_appts_this_year,
            follow_up_appts_last_year, total_paid_this_year, total_paid_last_year)

# ************************************************************************************
# ____________________________________________Begin Dash______________________________
# ************************************************************************************

st.markdown(
    """
    <style>
    section[data-testid="stMain"] > div[data-testid="stMainBlockContainer"] {
        max-width: 1800px; # Adjust this value as needed
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# set page config
st.set_page_config(page_title="Cumulative Onboarding", page_icon="📈", layout='wide')
YTD_days = ( datetime.datetime.now(ZoneInfo("America/Los_Angeles")).date() - datetime.datetime(datetime.datetime.now(ZoneInfo("America/Los_Angeles")).year, 1, 1).date() ).days

# Load your hero image
try:
    hero_image = Image.open("public/images/hero.jpg") # Replace with your image path
except FileNotFoundError:
    st.error("Hero image not found. Please ensure 'your_hero_image.jpg' is in the same directory.")

# sidebar
with st.sidebar:
    with st.container():
        # Add a title and description
        st.title("Client Dashboard")
        # Display the hero image
        st.image(hero_image, width='stretch')
       
        # # create a button to refresh data
        # # Placeholder for success message
        # message_placeholder = st.empty()
        # if "button_timestamp" not in st.session_state:
        #     st.session_state['button_timestamp'] = None
        # if st.button("Refresh Data", icon='🔄', on_click=load_data, args=(True,)):
        #     st.session_state['button_timestamp'] = datetime.datetime.now(ZoneInfo("America/Los_Angeles"))
        #     # Show success message
        # # Show and auto-clear message for 5 seconds
        # if st.session_state['button_timestamp']:
        #     if datetime.datetime.now(ZoneInfo("America/Los_Angeles")) - st.session_state['button_timestamp'] < datetime.timedelta(seconds=5):
        #         message_placeholder.success("Data refreshed!", icon="✅")
        #     else:
        #         message_placeholder.empty()

        # # Show cached timestamp
        # if st.session_state['button_timestamp']:
        #     st.write(f"{st.session_state['button_timestamp'].strftime('%b %d %I:%M %p')}")

        with st.expander("About this dashboard", icon="ℹ️", expanded=False):
            st.markdown(
                """
                This dashboard tracks client onboarding over time, comparing recent periods to previous ones. 
                It helps visualize trends in new and follow-up appointments, as well as revenue generated.

                **Data Sources:**
                - Client and appointment data is sourced from IntakeQ via their API.
                - Data is processed using pandas and visualized with Plotly and Streamlit.

                **Metrics Explained:**
                - **New Appointments:** Count of first-time appointments scheduled.
                - **Follow-ups:** Count of subsequent appointments for existing clients.
                - **Revenue:** Total paid amount from appointments.

                **Usage:**
                - Select a time window using the pills at the top to filter data.
                - Click "Refresh Data" in the sidebar to fetch the latest information from IntakeQ.

                **Note:** Ensure your IntakeQ API key is set in Streamlit secrets for live data refresh.
                """
            )
        st.write(datetime.datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%c"))
# client_summary_totals = pd.read_csv('data/client_summary_totals.csv')
# client_summary_totals['TotalAppointments'] = client_summary_totals['TotalAppointments'].astype(int)
# client_summary_totals['TotalPaidAmount'] = client_summary_totals['TotalPaidAmount'].astype(float)

# selectors
labels = {
    1: "1D",
    7: "1W",
    10: "10D",
    14: "2W",
    21: "3W",
    30: "1M",
    45: "6W",
    60: "2M",
    75: "10W",
    90: "3M",
    180: "6M",
    YTD_days: "YTD",
    365: "1Y",
}
options = labels.keys()

# load data
df, appts = load_data(run_live=False)

days = None
# header container
with st.container():
    st.header('Performance Metrics Overview')
    days = st.pills(
        ':material/filter_list: filter',
        label_visibility='visible',
        options=options,
        default=30,
        format_func=lambda x: labels[x]
    )
# set limits for markers
if days and days < 60:
    show_markers = True
else:
    show_markers = False

# set current date
now = CURRENT_DATE

with st.container():
    if days:
        # set metric label
        metric_label = f'{days} days'
        if days == 1:
            metric_label = '24 hours'
        elif days == YTD_days:
            metric_label = 'YTD'
        else:
            metric_label = labels[days]

        # Appointment Analysis
        tab1, tab2, tab3 = st.tabs(["Total", "Practitioner1", "Practitioner2"])
        for practitioner_id, tab in [('', tab1), ('6105fb763ccbf8258ce2b10a', tab2), ('6480ab3560413d84eebe6f1a', tab3)]:
                # create header container for practioner metrics
            with tab:  
                if practitioner_id:
                    practitioner_appts = appts.copy()
                    practitioner_appts = practitioner_appts[practitioner_appts['PractitionerId'] == practitioner_id].sort_values(by='Date', ascending=False)
                else:
                    practitioner_appts = appts.copy()   
                (new_appts_this_year, new_appts_last_year, follow_up_appts_this_year,
                        follow_up_appts_last_year, total_paid_this_year, total_paid_last_year) = filter_data(practitioner_appts, days)
                delta_new_appts_pct = calc_delta(len(new_appts_this_year), len(new_appts_last_year))
                delta_follow_up_appts_pct = calc_delta(len(follow_up_appts_this_year), len(follow_up_appts_last_year))
                delta_paid_pct = calc_delta(total_paid_this_year, total_paid_last_year)
                
                header_col1, header_col2, header_col3 = st.columns([1,1,1])
                with header_col1:
                    st.metric(value=f"{millify(len(new_appts_this_year), precision=2)}", label=f"New appointments", 
                                delta=f"{delta_new_appts_pct*100:.1f}%")
                
                with header_col2:
                    st.metric(value=f"{millify(len(follow_up_appts_this_year), precision=2)}", label=f"Follow-ups", 
                                delta=f"{delta_follow_up_appts_pct*100:.1f}%")
                
                with header_col3:
                    st.metric(value=f"${millify(total_paid_this_year, precision=1)}", label=f"Revenue",
                                delta=f"{delta_paid_pct*100:.1f}%")

                        
        ############## Onboarding Analysis
        with st.expander(expanded=True, label=f"### Onboarding Analysis **{metric_label}**").container(border=False):        
            col1, col2, col3 = st.columns(3, border=False)
            # load data windows

        data, current_data, previous_data, previous_data2, previous_data3 = get_window_data(df, days)
            # with curr_tab:
            #     generate_streamlit_chart(current_data, previous_data, days)
                    # create window data

        # create current and previous chart
        for col, period, (curr, prev) in [(col1, 'current', (current_data, previous_data)), (col2, 'previous', (previous_data, previous_data2))]:
            combined_data, delta_pct, curr_line_color, curr_line_color_bg = create_combined_data(curr, prev)
            with col:
                if combined_data['combined_cumsum'].sum() > 0:
                    val = f"{curr['cumsum'].max()}"
                else:
                    val = '0'
                with st.container(border=False):
                    st.metric(label=period, 
                        value=val, 
                        delta=f"{delta_pct*100:.1f}%")
                    if val != '0':
                        with st.container(border=False):
                            generate_streamlit_chart(combined_data, show_markers, chart_id=period)

        # created combined chart
        days *= 2       
        data, current_data, previous_data, previous_data2, previous_data3 = get_window_data(df, days)
        combined_data, delta_pct, curr_line_color, curr_line_color_bg = create_combined_data(current_data, previous_data)
        if combined_data['combined_cumsum'].sum() > 0:
            with col3:
                if combined_data['combined_cumsum'].sum() > 0:
                    val = f"{current_data['cumsum'].max()}"
                else:
                    val = '0'
                st.metric(label=f"Onboarded last {days*2} days",  
                    value=val,  
                    delta=f"{delta_pct*100:.1f}%")
                if val != '0':
                    with st.container(border=False):
                        generate_streamlit_chart(combined_data, show_markers, chart_id='col3_chart')
                # with overall_tab:
                #     generate_streamlit_chart(current_data, previous_data, days)

    # No Filter - All time stats
    else:
        st.write("Choose a filter 👆")

        first_appt_date = appts['Date'].min().date()
        total_days = (CURRENT_DATE - first_appt_date).days
        st.markdown("### All Time")
        st.markdown(f"{appts['Date'].min().date().strftime('%a %b %d, %Y')} to {appts['Date'].max().date().strftime('%a %b %d, %Y')}")
        days = (now - appts['Date'].min().date()).days
        (new_appts_this_year, new_appts_last_year, follow_up_appts_this_year,
                        follow_up_appts_last_year, total_paid_this_year, total_paid_last_year) = filter_data(appts, days)
        delta_new_appts_pct = calc_delta(len(new_appts_this_year), len(new_appts_last_year))
        delta_follow_up_appts_pct = calc_delta(len(follow_up_appts_this_year), len(follow_up_appts_last_year))
        delta_paid_pct = calc_delta(total_paid_this_year, total_paid_last_year)
        
        with st.container():
            header_col1, header_col2, header_col3 = st.columns([1,1,1])

            with header_col1:
                st.metric(value=f"{millify(len(new_appts_this_year), precision=2)}", label=f"New appointments")
            
            with header_col2:
                st.metric(value=f"{millify(len(follow_up_appts_this_year), precision=2)}", label=f"Follow-ups")
            
            with header_col3:
                st.metric(value=f"${millify(total_paid_this_year, precision=1)}", label=f"Revenue")
    

    