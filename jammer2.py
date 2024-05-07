import requests
from sqlalchemy import create_engine, text, Column, Integer, String, ForeignKey, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

password = 'Nimo_db_password'
bd = "gps_jamming"
connection_string = f'mysql+pymysql://admin:{password}@database-1.c1em0ggakjnc.eu-north-1.rds.amazonaws.com:3306/{bd}'
engine = create_engine(connection_string)

Session = sessionmaker(bind=engine)
Base = declarative_base()
scheduler = BackgroundScheduler()


def api_call(lat=59.530270, lon=29.458939, radius=750):
    base_url = 'https://api.airplanes.live/v2/point/'
    url = f'{base_url}{lat}/{lon}/{radius}'
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()

def get_or_create(session, model, defaults=None, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance, False
    else:
        params = dict((k, v) for k, v in kwargs.items() if not isinstance(v, (list, dict)))
        params.update(defaults or {})
        instance = model(**params)
        try:
            session.add(instance)
            session.commit()
        except IntegrityError:
            session.rollback()
            instance = session.query(model).filter_by(**kwargs).first()
            return instance, False
        return instance, True

def collect_data():
    data = api_call()

    now_timestamp = data.get('now')
    now_datetime = datetime.datetime.fromtimestamp(now_timestamp / 1000.0)
    now_datetime = now_datetime.replace(microsecond=0)

    session = Session()

    try:
        last_fetch = session.execute(text("SELECT MAX(timestamp) FROM flight")).fetchone()
        last_timestamp_str = last_fetch[0]
        if last_timestamp_str:
            last_timestamp_datetime = datetime.datetime.strptime(last_timestamp_str, '%Y-%m-%d %H:%M:%S')
            if now_datetime == last_timestamp_datetime:
                print(f"No new update from API - No timestamp difference\n{'-' * 45}\nLastest timestamp: {last_timestamp_datetime}")
                return
        else:
            print('db empty')
    except Exception as e:
        print(f'Error querying database: {e}')
        return
    finally:
        session.close()

    aircraft_list = []

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
            'altitude': item.get('alt_baro', 0),
            'speed': item.get('ias', 0),
            'heading': item.get('track', 0),
            'gpsOkBefore': item.get('gpsOkBefore', 0),
            'timestamp': now_datetime
        }
        aircraft_list.append(dict_info)

    try:
        for aircraft in aircraft_list:
            airline, _ = get_or_create(session, Airline, airline_code=aircraft['airline'])
            aircraft_type, _ = get_or_create(session, AircraftType, aircraft_type=aircraft['type'][:45], aircraft_desc=aircraft['aircraft'][:45])
            
            flight_id = aircraft['hex']
            flight = session.query(Flight).filter_by(id_flight=flight_id).first()
            if flight:
                flight.timestamp = aircraft['timestamp']
            else:
                flight = Flight(
                    id_flight=flight_id,
                    reg=aircraft['reg'],
                    callsign=aircraft['flight'].strip(),
                    timestamp=aircraft['timestamp'],
                    id_airline=airline.id_airline,
                    id_type=aircraft_type.id_type
                )
                session.add(flight)
            session.commit()

            geolocation = Geolocation(
                id_flight=flight_id,
                latitude=aircraft['lat'],
                longitude=aircraft['lon'],
                altitude=-1 if aircraft['altitude'] == 'ground' else aircraft['altitude'],
                speed=aircraft['speed'],
                heading=aircraft['heading'],
                nac_p=aircraft['nac_p'],
                nac_v=aircraft['nac_v'],
                nic=aircraft['nic'],
                nic_baro=aircraft['nic_baro'],
                gpsOkBefore=aircraft['gpsOkBefore'],
                timestamp=aircraft['timestamp']
            )
            session.add(geolocation)
            session.commit()

    except Exception as e:
        session.rollback()
        print(f"Error inserting data: {e}")
    finally:
        session.close()
        print(f'Updated on: {now_datetime}\n{"-" * 45}')

class Airline(Base):
    __tablename__ = 'airline'
    id_airline = Column(Integer, primary_key=True)
    airline_code = Column(String(255), nullable=False, unique=True)

class AircraftType(Base):
    __tablename__ = 'aircraft_type'
    id_type = Column(Integer, primary_key=True)
    aircraft_type = Column(String(255), nullable=False, unique=True)
    aircraft_desc = Column(String(255))

class Flight(Base):
    __tablename__ = 'flight'
    id_flight = Column(String(255), primary_key=True)
    reg = Column(String(255))
    callsign = Column(String(255))
    timestamp = Column(DateTime)
    id_airline = Column(Integer, ForeignKey('airline.id_airline'))
    id_type = Column(Integer, ForeignKey('aircraft_type.id_type'))

class Geolocation(Base):
    __tablename__ = 'geolocation'
    id = Column(Integer, primary_key=True)
    id_flight = Column(String(255), ForeignKey('flight.id_flight'))
    latitude = Column(Float)
    longitude = Column(Float)
    altitude = Column(Float)
    speed = Column(Float)
    heading = Column(Float)
    nac_p = Column(Float)
    nac_v = Column(Float)
    nic = Column(Float)
    nic_baro = Column(Float)
    gpsOkBefore = Column(Float)
    timestamp = Column(DateTime)


scheduler.add_job(collect_data, 'interval', seconds=30)
scheduler.start()

if __name__ == '__main__':
    app.run()
