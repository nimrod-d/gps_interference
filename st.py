import streamlit as st
import repository as repo
import data_fetch
from datetime import timedelta, datetime
from apscheduler.schedulers.background import BackgroundScheduler

########################################################
st.title(':blue[GPS] Interference')

min_timestamp, max_timestamp = repo.get_min_max_timestamp()

# return values from two-way slider to use for requesting data between dates
min_date, max_date = st.slider('Date Slider',
                               value=[max_timestamp, min_timestamp],
                               step=timedelta(days=0.01))
st.write(min_date, max_date)

show_affected = st.sidebar.checkbox('Show Affected Only')

# Get plane location data based on user inputs
map_data = repo.get_plane_location(min_date, max_date, show_affected)

# % of planes experiencing low nac_p(<=6)
p_interference = repo.p_of_interference(min_date, max_date)

# Display map with plane locations
st.map(map_data, latitude='latitude', longitude='longitude', color='affected_color')
st.line_chart(p_interference, x='timestamp', y='p_interference')
####################################################################################
# EXECUTION BLOCK
####################################################################################
scheduler = BackgroundScheduler()
scheduler.add_job(data_fetch.collect_data, 'interval', minutes=1)
collect = st.sidebar.checkbox('Live Data Collection', value=False)

try:
    if collect:
        scheduler.start()

    while collect:
        st.write('Live Data Collection')
        collect = st.sidebar.toggle('Live Data Collection', value=False)

    scheduler.shutdown()
except:
    st.write('No Live Data Collection')
