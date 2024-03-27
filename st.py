import streamlit as st
import repository as repo
import data_fetch
from datetime import timedelta, datetime
from apscheduler.schedulers.background import BackgroundScheduler

########################################################
st.title(':blue[GPS] Interference')

# % of planes experiencing low nac_p(<=6)
p_interference = repo.p_of_interference()

min_timestamp, max_timestamp = repo.get_min_max_timestamp()

# min_date, max_date = st.slider('timestamp',
#                                value=[max_timestamp, min_timestamp],
#                                step=timedelta(days=0.01))
# st.write(min_date, max_date)

# map_data = repo.get_plane_location(min_date, max_date)

# st.map(map_data, latitude='latitude', longitude='longitude', color='affected_color')
# Sidebar widgets
min_time = st.sidebar.date_input('Start Date', value=datetime.now())
max_time = st.sidebar.date_input('End Date', value=datetime.now())
show_affected = st.sidebar.checkbox('Show Affected Only')

# Get plane location data based on user inputs
map_data = repo.get_plane_location(min_timestamp, max_timestamp, show_affected)

# Display map with plane locations
st.map(map_data, latitude='latitude', longitude='longitude', color='affected_color')
####################################################################################
# EXECUTION BLOCK
####################################################################################
scheduler = BackgroundScheduler()
scheduler.add_job(data_fetch.collect_data, 'interval', minutes=1)
collect = st.sidebar.checkbox('Live Data Collection', value=False)

if collect:
    scheduler.start()

while collect:
    st.write('Live Data Collection')
    collect = st.sidebar.toggle('Live Data Collection', value=False)

scheduler.shutdown()
