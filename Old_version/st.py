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
with st.sidebar:
    st.title('Data Collection')
    collect_baltic = st.checkbox('Collect Data Baltic sea')
    collect_israel = st.checkbox('Collect Data Israel')
    collect = False
    while collect_israel != collect_baltic:
        collect = True

    st.title('Affected')
    show_affected = st.checkbox('Show Affected Only')

# Get plane location data based on user inputs
map_data = repo.get_plane_location(min_date, max_date, show_affected)

# % of planes experiencing low nac_p(<=6)
p_interference = repo.p_of_interference(min_date, max_date)

################################ TESTING #########################################


# p_interference.sort_values(by=['timestamp'], ascending=False, inplace=True)
# print(p_interference)
# x = p_interference['count_affected'].iloc[0]
# print(x)

#################################################################################
# Display map with plane locations
# st.map(map_data, latitude='latitude', longitude='longitude', color='affected_color')
# # plot max(p_interference) -- count of all planes -- count affected
#
# st.line_chart(p_interference, x='timestamp', y='p_interference')


####################################################################################
# EXECUTION BLOCK
####################################################################################
# scheduler = BackgroundScheduler()
# scheduler.add_job(data_fetch.collect_data, 'interval', minutes=1)
#
# while collect:
#     st.write('Live Data Collection')
#     scheduler.start()
#
# else:
#     scheduler.shutdown()
