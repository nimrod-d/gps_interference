import requests
from sqlalchemy import create_engine
import datetime
import warnings
from datetime import datetime
import os
import st

warnings.filterwarnings('ignore')
sql_password = os.environ.get('SQL_PASSWORD')

bd = "gps_jamming"
connection_string = 'mysql+pymysql://root:' + sql_password + '@localhost:3306/' + bd
engine = create_engine(connection_string)
print(engine)


def api_call(lat=55.620824, lon=17.771781, radius=200):
    """
    API call with default location set to the middle of baltic sea
    can be changed at function call
    """

    base_url = 'https://api.airplanes.live/v2/point/'

    # if st.collect_israel:
    #     url = f'{base_url}{33.951715}/{34.684787}/{radius}'
    # else:
    #     url = f'{base_url}{lat}/{lon}/{radius}'

    url = f'{base_url}{33.951715}/{34.684787}/{radius}'
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
    else:
        print('error', response.status_code)

    return data


def get_airline_id(airline_code):
    with engine.connect() as connection:
        result = connection.execute(
            "SELECT id_airline FROM airline WHERE airline_code = %s",
            (airline_code,)
        ).fetchone()
        return result[0] if result else None


def get_aircraft_type_id(aircraft_type):
    with engine.connect() as connection:
        result = connection.execute(
            "SELECT id_type FROM aircraft_type WHERE aircraft_type = %s",
            (aircraft_type,)
        ).fetchone()
        return result[0] if result else None


def get_flight_id(flight_id):
    with engine.connect() as connection:
        result = connection.execute(
            "SELECT id_flight FROM flight WHERE id_flight = %s",
            (flight_id,)
        ).fetchone()
        return result[0] if result else None


def get_last_timestamp():
    with engine.connect() as connection:
        last_fetch = connection.execute(
            "SELECT MAX(timestamp) FROM flight"
        ).fetchone()
    last_timestamp_str = last_fetch[0]
    if last_timestamp_str:
        last_timestamp_datetime = datetime.strptime(last_timestamp_str, '%Y-%m-%d %H:%M:%S')
        return last_timestamp_datetime
    else:
        return None


def flight_insert(flight_data):
    updated_count = 0
    new_count = 0

    for flight_info in flight_data:
        hex_id = flight_info.get('hex', '')

        # Check if the flight already exists based on the 'hex' identifier
        flight_id = get_flight_id(hex_id)

        if flight_id is not None:
            # Flight already exists, update its timestamp
            with engine.connect() as connection:
                try:
                    timestamp_str = flight_info['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                    connection.execute(
                        "UPDATE flight SET timestamp = %s WHERE id_flight = %s",
                        (timestamp_str, flight_id)
                    )
                    updated_count += 1
                except Exception as e:
                    print(f"Error updating flight timestamp: {e}")
        else:
            # Flight does not exist, insert it into the 'flight' table
            airline_code = flight_info.get('airline', '')
            aircraft_type = flight_info.get('type', '')

            # Get airline and aircraft type IDs
            airline_id = get_airline_id(airline_code)
            aircraft_type_id = get_aircraft_type_id(aircraft_type)

            if airline_id is None:
                print(f"Error: Airline with code '{airline_code}' not found.")
                continue
            if aircraft_type_id is None:
                print(f"Error: Aircraft type '{aircraft_type}' not found.")
                continue

            timestamp_str = flight_info['timestamp'].strftime('%Y-%m-%d %H:%M:%S')

            # Insert flight data into the 'flight' table
            with engine.connect() as connection:
                try:
                    connection.execute(
                        "INSERT INTO flight (id_flight, reg, callsign, timestamp, id_airline, id_type) VALUES (%s, "
                        "%s, %s, %s, %s, %s)",
                        (hex_id, flight_info['reg'], flight_info['flight'].strip(), timestamp_str, airline_id,
                         aircraft_type_id)
                    )
                    new_count += 1
                except Exception as e:
                    print(f"Error inserting flight data: {e}")

    print(f"Updated {updated_count} flights' timestamps. Inserted {new_count} new flights.\n{'-' * 45}")


def airline_insert(aircraft_list):
    new_airline_count = 0
    existing_airline_count = 0

    for aircraft in aircraft_list:
        airline_code = aircraft['airline']
        with engine.connect() as connection:
            airline_exists = connection.execute(
                "SELECT id_airline FROM airline WHERE airline_code = %s",
                (airline_code,)
            ).fetchone()

        if airline_exists:
            existing_airline_count += 1
        else:
            with engine.connect() as connection:
                try:
                    connection.execute(
                        "INSERT INTO airline (id_airline, airline_code) VALUES (DEFAULT, %s)",
                        (airline_code,)
                    )
                    print(f'Airline {airline_code} inserted successfully.')
                    new_airline_count += 1
                except Exception as e:
                    print(f'Error inserting airline {airline_code}: {e}')

    print(
        f'Existing airlines in database: {existing_airline_count}\nAdded airlines to database: {new_airline_count}\n\n')


def aircraft_type_insert(aircraft_types):
    new_type_count = 0
    existing_type_count = 0

    for aircraft_type in aircraft_types:
        type_value = aircraft_type['type'][:45]
        description_value = aircraft_type['aircraft'][:45]

        with engine.connect() as connection:
            # Check if aircraft type already exists
            aircraft_type_exists = connection.execute(
                "SELECT id_type FROM aircraft_type WHERE aircraft_type = %s",
                (type_value,)
            ).fetchone()

        if aircraft_type_exists:
            existing_type_count += 1
        else:
            with engine.connect() as connection:
                try:
                    connection.execute(
                        "INSERT INTO aircraft_type (aircraft_type, aircraft_desc) VALUES (%s, %s)",
                        (type_value, description_value)
                    )
                    print(f'Aircraft type {type_value} inserted successfully.')
                    new_type_count += 1
                except Exception as e:
                    print(f'Error inserting aircraft type {type_value}: {e}')

    print(f'Existing in database: {existing_type_count}\nAdded type to database: {new_type_count}\n\n')


def insert_geolocation_data(geolocation_data):
    added_count = 0

    for data_point in geolocation_data:
        flight_hex = data_point.get('hex', '')

        # Check if the flight exists in the flight table
        id_flight = get_flight_id(flight_hex)
        if id_flight is None:
            print(f"Error: Flight with hex '{flight_hex}' not found.")
            continue

        # Extract geolocation data
        latitude = data_point.get('lat', 0)
        longitude = data_point.get('lon', 0)
        altitude = data_point.get('alt_baro', 0)
        speed = data_point.get('ias', 0)
        heading = data_point.get('track', 0)
        nac_p = data_point.get('nac_p', 0)
        nac_v = data_point.get('nac_v', 0)
        nic = data_point.get('nic', 0)
        nic_baro = data_point.get('nic_baro', 0)
        gpsOkBefore = data_point.get('gpsOkBefore', 0)
        timestamp = data_point.get('timestamp', '')

        if altitude == 'ground':
            altitude = -1

        # Insert geolocation data into the geolocation table
        with engine.connect() as connection:
            try:
                connection.execute(
                    '''INSERT INTO geolocation (id_flight,
                    latitude,
                    longitude,
                    altitude,
                    speed,
                    heading,
                    nac_p,
                    nac_v,
                    nic,
                    nic_baro,
                    gpsOkBefore,
                    timestamp) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                    (id_flight,
                     latitude,
                     longitude,
                     altitude,
                     speed,
                     heading,
                     nac_p,
                     nac_v,
                     nic,
                     nic_baro,
                     gpsOkBefore,
                     timestamp)
                )
                added_count += 1
            except Exception as e:
                print(f"Error inserting geolocation data: {e}")

    print(f"{added_count} geolocation data points inserted successfully.")


def collect_data():
    # operation

    data = api_call()

    # Getting timestamp from last API call 
    now_timestamp = data.get('now')
    now_datetime = datetime.fromtimestamp(now_timestamp / 1000.0)
    now_datetime = now_datetime.replace(microsecond=0)

    # Getting latest timestamp in database
    latest_timestamp = get_last_timestamp()

    aircraft_list = []

    if now_datetime != latest_timestamp:
        for item in data['ac']:
            dict_info = {
                'hex': item.get('hex', ''),
                'reg': item.get('r', ''),
                'airline': item.get('ownOp', ''),
                'flight': item.get('flight', ''),
                'type': item.get('t', ''),
                'aircraft': item.get('desc', ''),
                'nac_p': item.get('nac_p', 0),
                'nic': item.get('nic', ''),
                'nic_baro': item.get('nic_baro', 0),
                'nac_v': item.get('nac_v', 0),
                'lat': item.get('lat', 0),
                'lon': item.get('lon', 0),
                'alt_baro': item.get('alt_baro', 0),
                'ias': item.get('ias', 0),
                'track': item.get('track', 0),
                'gpsOkBefore': item.get('gpsOkBefore', 0),
                'timestamp': now_datetime
            }
            aircraft_list.append(dict_info)

        # Data Process & insert to DB        
        airline_insert(aircraft_list)
        aircraft_type_insert(aircraft_list)
        flight_insert(aircraft_list)
        insert_geolocation_data(aircraft_list)

    else:
        print(f"No new update from API - No timestamp difference\n{'-' * 45}\nLatest timestamp: {latest_timestamp}")


# EXECUTION
try:
    from apscheduler.schedulers.background import BackgroundScheduler

    # Create a scheduler
    scheduler = BackgroundScheduler()

    # Schedule the data collection function to run every hour
    scheduler.add_job(collect_data, 'interval', minutes=1)

    # Start the scheduler
    scheduler.start()
    # scheduler.shutdown()
except:
    print('Did not work')

# scheduler.shutdown()
