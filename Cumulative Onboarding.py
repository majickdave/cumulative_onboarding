import streamlit as st
import pandas as pd
import numpy as np
import datetime
# import from code folder
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent / 'python_functions'))
from dashboard_functions import get_window_data
from dashboard_functions import load_data
from dashboard_functions import generate_streamlit_chart
from dashboard_functions import create_combined_data
from dashboard_functions import render_metric_card
import plotly.express as px
from dashboard_functions import display_clock

# load data
df = load_data()

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