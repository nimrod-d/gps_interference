from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import os

sql_password = os.environ.get('SQL_PASSWORD')
time_format = '%Y-%m-%d %H:%M:%S'

bd = "gps_jamming"
connection_string = 'mysql+pymysql://root:' + sql_password + '@localhost:3306/' + bd
engine = create_engine(connection_string)
print(engine)


# Get Max timestamp

def get_last_timestamp():
    with engine.connect() as connection:
        last_fetch = connection.execute(
            "SELECT MAX(timestamp) FROM flight"
        ).fetchone()
    last_timestamp_str = last_fetch[0]
    if last_timestamp_str:
        last_timestamp_datetime = datetime.strptime(last_timestamp_str, time_format)
        return last_timestamp_datetime
    else:
        return None


def get_count_of_nac_p_under_6():
    with engine.connect() as connection:
        result = connection.execute(
            """
        SELECT geolocation.timestamp, COUNT(id_flight) AS flight_count
        FROM flight 
        INNER JOIN geolocation
        USING(id_flight)
        WHERE nac_p <= 6
        GROUP BY timestamp;
        """
        )
        data = result.fetchall()

    return data


# Get the count of total planes for given timestamp

def get_count_all_planes():
    with engine.connect() as connection:
        result = connection.execute(
            '''
        SELECT geolocation.timestamp, COUNT(id_flight) AS flight_count
        FROM flight 
        INNER JOIN geolocation
        USING(id_flight)
        GROUP BY timestamp;
        '''
        )
        data = result.fetchall()
    return data


# data pull for plotting interference % (low nac_p) over time using functions above

def p_of_interference(min_time, max_time):
    mi = min_time.strftime(time_format)
    mx = max_time.strftime(time_format)

    count_total_flights = get_count_all_planes()
    count_affected_flights = get_count_of_nac_p_under_6()

    total_df = pd.DataFrame(count_total_flights, columns=['timestamp', 'total_count'])
    affected_df = pd.DataFrame(count_affected_flights, columns=['timestamp', 'count_affected'])

    merged_df = pd.merge(total_df, affected_df, on='timestamp', how='outer')
    merged_df['p_interference'] = (merged_df['count_affected'] / merged_df['total_count']) * 100
    df = merged_df[(merged_df['timestamp'] >= mi) & (merged_df['timestamp'] <= mx)]

    return df


# Function to fetch plane location data
def get_plane_location(min_time, max_time, show_affected):
    print(min_time, max_time)
    mi = min_time.strftime(time_format)
    mx = max_time.strftime(time_format)
    print(min_time, max_time)

    if show_affected:
        query = f'''
            SELECT f.id_flight, f.callsign, g.latitude, g.longitude, g.nac_p, g.timestamp
            FROM geolocation g
            INNER JOIN flight f
            USING(id_flight)
            WHERE g.timestamp BETWEEN '{mi}' AND '{mx}' AND g.nac_p <= 6
        '''
    else:
        query = f'''
            SELECT f.id_flight, f.callsign, g.latitude, g.longitude, g.nac_p, g.timestamp
            FROM geolocation g
            INNER JOIN flight f
            USING(id_flight)
            WHERE g.timestamp BETWEEN '{mi}' AND '{mx}'
        '''

    # Execute query and fetch data
    with engine.connect() as connection:
        result = connection.execute(query)
        data = result.fetchall()

    location_df = pd.DataFrame(data, columns=['id_flight', 'callsign', 'latitude', 'longitude', 'nac_p', 'timestamp'])

    location_df['affected_color'] = location_df['nac_p'].apply(lambda x: "#F02070" if x <= 6 else '#20F0A0')
    location_df['latitude'] = location_df['latitude'].astype(float)
    location_df['longitude'] = location_df['longitude'].astype(float)

    return location_df


def get_min_max_timestamp():
    with engine.connect() as connection:
        min_fetch = connection.execute("SELECT MIN(timestamp) FROM flight").fetchone()
        min_timestamp = datetime.strptime(min_fetch[0], time_format) if min_fetch else None

        max_fetch = connection.execute("SELECT MAX(timestamp) FROM flight").fetchone()
        max_timestamp = datetime.strptime(max_fetch[0], time_format) if max_fetch else None

    return min_timestamp, max_timestamp

# min_, max_ = get_min_max_timestamp()
# print("min_time",min_,"max_time", max_)
#
#
# get_plane_location(min_,max_)
