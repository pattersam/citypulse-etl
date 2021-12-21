"""Data model classes"""

import os
import json
import pandas as pd

from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    Float,
    DateTime,
    UniqueConstraint
)
from sqlalchemy.ext.declarative import declarative_base

from .database import db_engine
from .utils import url_to_filename, check_for_header

import logging
log = logging.getLogger(__name__)

Base = declarative_base()


# Primary Data Type Models

class RoadTrafficData(Base):
    """Road Traffic Data"""

    __tablename__ = 'road_traffic_data'

    # Column definitions
    id = Column(Integer, primary_key=True)
    status = Column(String)
    avg_measured_time = Column(Float)
    avg_speed = Column(Float)
    ext_id = Column(Integer)
    median_measured_time = Column(Float)
    timestamp = Column(DateTime)
    vehicle_count = Column(Float)
    report_id = Column(Integer, ForeignKey('traffic_sensors.id'))
    dataset_id = Column(Integer, ForeignKey('datasets.id'))

    # Uniqueness constraints
    __table_args__ = (
        UniqueConstraint(
            'timestamp',
            'report_id',
            name='_time_uc'
            ),
    )

    raw_data_column_map = {
        'status': 'status',
        'avgMeasuredTime': 'avg_measured_time',
        'avgSpeed': 'avg_speed',
        'extID': 'ext_id',
        'medianMeasuredTime': 'median_measured_time',
        'TIMESTAMP': 'timestamp',
        'vehicleCount': 'vehicle_count',
        '_id': 'source_id',
        'REPORT_ID': 'report_id',
    }

    @classmethod
    def read_raw_data(cls, fname, dataset):
        assert fname.endswith('.csv')
        if check_for_header(fname):
            return pd.read_csv(fname)
        else:
            return pd.read_csv(fname, names = cls.raw_data_column_map.keys())

    
    @classmethod
    def validate_raw_data(cls, df):
        missing_cols = set(cls.raw_data_column_map.keys()).difference(set(df.columns))
        if missing_cols:
            msg = f"Raw data missing columns: {missing_cols}"
            log.error(msg)
            raise ValueError(msg)

    @classmethod
    def transform_raw_data(cls, df, dataset):
        df = df.drop_duplicates().reset_index(drop=True)
        df['dataset_id'] = dataset.id
        df = df.rename(columns=cls.raw_data_column_map)
        df['timestamp']= pd.to_datetime(df['timestamp'])
        return df[[c.name for c in cls.__table__.c if c.name != 'id']]


class PollutionData(Base):
    """Pollution Data"""

    __tablename__ = 'pollution_data'

    # Column definitions
    id = Column(Integer, primary_key=True)
    ozone = Column(Float)
    particullate_matter = Column(Float)
    carbon_monoxide = Column(Float)
    sulfure_dioxide = Column(Float)
    nitrogen_dioxide = Column(Float)
    longitude = Column(Float)
    latitude = Column(Float)
    timestamp = Column(DateTime)
    report_id = Column(Integer, ForeignKey('traffic_sensors.id'))
    dataset_id = Column(Integer, ForeignKey('datasets.id'))

    # Uniqueness constraints
    __table_args__ = (
        UniqueConstraint(
            'longitude',
            'latitude',
            'timestamp',
            'report_id',
            name='_space_time_uc'
            ),
    )

    raw_data_column_map = {
        'ozone': 'ozone',
        'particullate_matter': 'particullate_matter',
        'carbon_monoxide': 'carbon_monoxide',
        'sulfure_dioxide': 'sulfure_dioxide',
        'nitrogen_dioxide': 'nitrogen_dioxide',
        'longitude': 'longitude',
        'latitude': 'latitude',
        'timestamp': 'timestamp',
    }

    @classmethod
    def read_raw_data(cls, fname, dataset):
        assert fname.endswith('.csv')
        if check_for_header(fname):
            df = pd.read_csv(fname)
        else:
            df = pd.read_csv(fname, names = cls.raw_data_column_map.keys())
        df['report_id'] = int(fname.split('Data')[-1][:-4])
        return df

    @classmethod
    def validate_raw_data(cls, df):
        missing_cols = set(cls.raw_data_column_map.keys()).difference(set(df.columns))
        if missing_cols:
            msg = f"Raw data missing columns: {missing_cols}"
            log.error(msg)
            raise ValueError(msg)

    @classmethod
    def transform_raw_data(cls, df, dataset):
        df['dataset_id'] = dataset.id
        df = df.rename(columns=cls.raw_data_column_map)
        df['timestamp']= pd.to_datetime(df['timestamp'])
        return df[[c.name for c in cls.__table__.c if c.name != 'id']]


class WeatherData(Base):
    """Weather Data"""

    __tablename__ = 'weather_data'

    # Column definitions
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    dew_point = Column(Float)
    pressure = Column(Float)
    wind_direction = Column(Float)
    temperature = Column(Float)
    visibility = Column(Float)
    wind_speed = Column(Float)
    humidity = Column(Float)
    dataset_id = Column(Integer, ForeignKey('datasets.id'))


    # Uniqueness constraints
    __table_args__ = (
        UniqueConstraint(
            'timestamp',
            'dataset_id',
            name='_weather_uc'
            ),
    )

    raw_data_column_map = {
        'timestamp': 'timestamp',
        'dewptm': 'dew_point',
        'pressurem': 'pressure',
        'wdird': 'wind_direction',
        'tempm': 'temperature',
        'vism': 'visibility',
        'wspdm': 'wind_speed',
        'hum': 'humidity',
    }

    @classmethod
    def read_raw_data(cls, fname, dataset):
        assert fname.endswith('.txt'), f"{fname} is not a `.txt` file"
        variable = os.path.split(fname)[-1].split('.')[0]
        data = []
        with open(fname, 'r') as f:
            for line in f.readlines():
                data.extend([
                    {'timestamp': t, variable: float(v) if v else None}
                    for t,v in json.loads(line).items()
                    ])
        df = pd.DataFrame.from_records(data)
        return df

    @classmethod
    def validate_raw_data(cls, df):
        missing_cols = set(cls.raw_data_column_map.keys()).difference(set(df.columns))
        assert 'timestamp' not in missing_cols
        # we only get 2 columns at a time from this data
        assert len(missing_cols) == (len(cls.raw_data_column_map) - 2)

    @classmethod
    def transform_raw_data(cls, df, dataset):
        df['dataset_id'] = dataset.id
        df = df.rename(columns=cls.raw_data_column_map)
        df['timestamp']= pd.to_datetime(df['timestamp'])
        return df[[c.name for c in cls.__table__.c if c.name != 'id' and c.name in df.columns]]


class ParkingData(Base):
    """Parking Data"""

    __tablename__ = 'parking_data'

    # Column definitions
    id = Column(Integer, primary_key=True)
    vehicle_count = Column(Integer)
    timestamp = Column(DateTime)
    _id = Column(Integer)
    total_spaces = Column(Integer)
    garage_code = Column(String, ForeignKey('parking_lots.garage_code'))
    stream_time = Column(DateTime)
    dataset_id = Column(Integer, ForeignKey('datasets.id'))

    # Uniqueness constraints
    __table_args__ = (
        UniqueConstraint(
            'garage_code',
            'timestamp',
            name='_space_time_uc'
            ),
    )

    raw_data_column_map = {
        'vehiclecount': 'vehicle_count',
        'updatetime': 'timestamp',
        '_id': '_id',
        'totalspaces': 'total_spaces',
        'garagecode': 'garage_code',
        'streamtime': 'stream_time',
    }

    @classmethod
    def read_raw_data(cls, fname, dataset):
        assert fname.endswith('.csv')
        if check_for_header(fname):
            return pd.read_csv(fname)
        else:
            return pd.read_csv(fname, names = cls.raw_data_column_map.keys())

    @classmethod
    def validate_raw_data(cls, df):
        missing_cols = set(cls.raw_data_column_map.keys()).difference(set(df.columns))
        if missing_cols:
            msg = f"Raw data missing columns: {missing_cols}"
            log.error(msg)
            raise ValueError(msg)

    @classmethod
    def transform_raw_data(cls, df, dataset):
        df['dataset_id'] = dataset.id
        df = df.rename(columns=cls.raw_data_column_map)
        df['timestamp']= pd.to_datetime(df['timestamp'])
        df['stream_time']= pd.to_datetime(df['stream_time'])
        return df[[c.name for c in cls.__table__.c if c.name != 'id']]


class SocialEventData(Base):
    """Social Event Data"""

    __tablename__ = 'social_event_data'

    # Column definitions
    id = Column(Integer, primary_key=True)
    name = Column(String)  # e.g. "Planning and Regulatory Committee"
    url = Column(String)  # e.g. http://www.surreycc.public-i.tv/core/portal/webcast_interactive/144043
    description = Column(String)  # "Planning and Regulatory Committee 03/09/2014 10.30 am Ashcombe Suite County Hall Kingston upon Thames Surrey KT1 2DN"
    timestamp = Column(DateTime)  # e.g. "Wed 03 Sep 2014 10:30:00 +0100"
    dataset_id = Column(Integer, ForeignKey('datasets.id'))

    # Uniqueness constraints
    __table_args__ = (
        UniqueConstraint(
            'name',
            'url',
            name='_event_uc'
            ),
    )

    raw_data_column_map = {
        'name': 'name',
        'url': 'url',
        'description': 'description',
        'webcast': 'webcast',
        'timestamp': 'timestamp',
    }

    @classmethod
    def read_raw_data(cls, fname, dataset):
        assert fname.endswith('.csv')
        if check_for_header(fname):
            return pd.read_csv(fname)
        else:
            return pd.read_csv(fname, names = cls.raw_data_column_map.keys())

    @classmethod
    def validate_raw_data(cls, df):
        missing_cols = set(cls.raw_data_column_map.keys()).difference(set(df.columns))
        if missing_cols:
            msg = f"Raw data missing columns: {missing_cols}"
            log.error(msg)
            raise ValueError(msg)

    @classmethod
    def transform_raw_data(cls, df, dataset):
        df['dataset_id'] = dataset.id
        df = df.rename(columns=cls.raw_data_column_map)
        df['timestamp']= pd.to_datetime(df['timestamp'])
        return df[[c.name for c in cls.__table__.c if c.name != 'id']]


class CulturalEventData(Base):
    """Cultural Event Data"""

    __tablename__ = 'cultural_event_data'

    # Column definitions
    id = Column(Integer, primary_key=True)
    category_number = Column(Integer)
    city = Column(String)
    name = Column(String)
    url = Column(String)
    price = Column(String)
    created_time = Column(Integer)
    post_code = Column(Integer)
    longitude = Column(Float)
    event_id = Column(String, unique=True)
    xml = Column(String)
    street = Column(String)
    room = Column(String)
    timestamp = Column(DateTime)
    latitude = Column(Float)
    calendar_url = Column(String)
    _id = Column(Integer)
    event_type = Column(String)
    image_url = Column(String)
    genre = Column(String)
    dataset_id = Column(Integer, ForeignKey('datasets.id'))

    raw_data_column_map = {
        'category_number': 'category_number',  # e.g. 1
        'city': 'city',  # e.g. Aarhus C
        'name': 'name',  # e.g. KAMMERKONCERT
        'url': 'url',  # e.g. http://www.billetlugen.dk/referer/?r=266abe1b7fab4562a5c2531d0ae62171&p=/koeb/billetter/29048/46773/
        'price': 'price',  # e.g. 85.00 - 115.00 DKK
        'created_time': 'created_time',  # e.g. 1403593223
        'post_code': 'post_code',  # e.g. 8000
        'longitude': 'longitude',  # e.g. 10.19887
        'event_id': 'event_id',  # e.g. 46773
        'xml': 'xml',  # e.g. `<p><!--[if gte mso 9]>...`
        'street': 'street',  # e.g. Thomas Jensens AllÃ©
        'room': 'room',  # e.g. Symfonisk Sal
        'timestamp': 'timestamp',  # e.g. 2014-09-21T15:00:00
        'latitude': 'latitude',  # e.g. 56.1519158
        'calendar_url': 'calendar_url',  # e.g. http://www.musikhusetaarhus.dk/kalender/29048/
        '_id': '_id',  # e.g. 901
        'event_type': 'event_type',  # e.g. Musik
        'image_url': 'image_url',  # e.g. http://static.billetlugen.dk/images/events/b/29048.jpg
        'genre': 'genre',  # e.g. Klassisk
    }

    @classmethod
    def read_raw_data(cls, fname, dataset):
        assert fname.endswith('.csv')
        if check_for_header(fname):
            return pd.read_csv(fname)
        else:
            return pd.read_csv(fname, names = cls.raw_data_column_map.keys())

    @classmethod
    def validate_raw_data(cls, df):
        missing_cols = set(cls.raw_data_column_map.keys()).difference(set(df.columns))
        if missing_cols:
            msg = f"Raw data missing columns: {missing_cols}"
            log.error(msg)
            raise ValueError(msg)

    @classmethod
    def transform_raw_data(cls, df, dataset):
        df['dataset_id'] = dataset.id
        df = df.rename(columns=cls.raw_data_column_map)
        df['timestamp']= pd.to_datetime(df['timestamp'])
        return df[[c.name for c in cls.__table__.c if c.name != 'id']]


class LibraryEventData(Base):
    """Library Event Data"""

    __tablename__ = 'library_event_data'

    # Column definitions
    id = Column(Integer, primary_key=True)
    lid = Column(String)
    city = Column(String)
    end_time = Column(DateTime)
    title = Column(String)
    url = Column(String)
    price = Column(String)
    changed = Column(DateTime)
    content = Column(String)
    zip_code = Column(Integer)
    library = Column(String)
    image_url = Column(String)
    teaser = Column(String)
    street = Column(String)
    status = Column(Integer)
    longitude = Column(Float)
    start_time = Column(DateTime)
    latitude = Column(Float)
    _id = Column(Integer)
    event_id = Column(Integer)
    stream_time = Column(DateTime)
    dataset_id = Column(Integer, ForeignKey('datasets.id'))

    # Uniqueness constraints
    __table_args__ = (
        UniqueConstraint(
            'title',
            'url',
            name='_event_uc'
            ),
    )

    raw_data_column_map = {
        'lid': 'lid',
        'city': 'city',
        'endtime': 'end_time',
        'title': 'title',
        'url': 'url',
        'price': 'price',
        'changed': 'changed',
        'content': 'content',
        'zipcode': 'zip_code',
        'library': 'library',
        'imageurl': 'image_url',
        'teaser': 'teaser',
        'street': 'street',
        'status': 'status',
        'longitude': 'longitude',
        'starttime': 'start_time',
        'latitude': 'latitude',
        '_id': '_id',
        'id': 'event_id',
        'streamtime': 'stream_time',
    }

    @classmethod
    def read_raw_data(cls, fname, dataset):
        assert fname.endswith('.csv')
        if check_for_header(fname):
            return pd.read_csv(fname)
        else:
            return pd.read_csv(fname, names = cls.raw_data_column_map.keys())

    @classmethod
    def validate_raw_data(cls, df):
        missing_cols = set(cls.raw_data_column_map.keys()).difference(set(df.columns))
        if missing_cols:
            msg = f"Raw data missing columns: {missing_cols}"
            log.error(msg)
            raise ValueError(msg)

    @classmethod
    def transform_raw_data(cls, df, dataset):
        df['dataset_id'] = dataset.id
        df = df.rename(columns=cls.raw_data_column_map)
        df['end_time'] = pd.to_datetime(df['end_time'])
        df['changed'] = pd.to_datetime(df['changed'])
        df['start_time'] = pd.to_datetime(df['start_time'])
        df['stream_time'] = pd.to_datetime(df['stream_time'])
        return df[[c.name for c in cls.__table__.c if c.name != 'id']]


data_type_registry = {
    'Road Traffic Data': RoadTrafficData,
    'Pollution Data': PollutionData,
    'Weather Data': WeatherData,
    'Parking Data': ParkingData,
    'Social Event Data': SocialEventData,
    'Cultural Event Data': CulturalEventData,
    'Library Event Data': LibraryEventData,
}

# Meta Data Models

class TrafficSensor(Base):

    __tablename__ = "traffic_sensors"

    # Column definitions
    id = Column(String, unique=True, primary_key=True)
    point_1_street = Column(String)
    duration_in_sec = Column(Float)
    point_1_name = Column(String)
    point_1_city = Column(String)
    point_2_name = Column(String)
    point_2_longitude = Column(Float)
    point_2_street = Column(String)
    ndt_in_kmh = Column(Float)
    point_2_postal_code = Column(Integer)
    point_2_country = Column(String)
    point_1_street_number = Column(String)
    organisation = Column(String)
    point_1_latitude = Column(Float)
    point_2_latitude = Column(Float)
    point_1_postal_code = Column(Integer)
    point_2_street_number = Column(String)
    point_2_city = Column(String)
    extid = Column(Integer)
    road_type = Column(String)
    point_1_longitude = Column(Float)
    report_id = Column(Integer)
    report_name = Column(String)
    point_1_country = Column(String)
    distance_in_meters = Column(Float)
    rba_id = Column(String)
    source_id  = Column(String)

    raw_data_column_map = {
        'POINT_1_STREET': 'point_1_street',
        'DURATION_IN_SEC': 'duration_in_sec',
        'POINT_1_NAME': 'point_1_name',
        'POINT_1_CITY': 'point_1_city',
        'POINT_2_NAME': 'point_2_name',
        'POINT_2_LNG': 'point_2_longitude',
        'POINT_2_STREET': 'point_2_street',
        'NDT_IN_KMH': 'ndt_in_kmh',
        'POINT_2_POSTAL_CODE': 'point_2_postal_code',
        'POINT_2_COUNTRY': 'point_2_country',
        'POINT_1_STREET_NUMBER': 'point_1_street_number',
        'ORGANISATION': 'organisation',
        'POINT_1_LAT': 'point_1_latitude',
        'POINT_2_LAT': 'point_2_latitude',
        'POINT_1_POSTAL_CODE': 'point_1_postal_code',
        'POINT_2_STREET_NUMBER': 'point_2_street_number',
        'POINT_2_CITY': 'point_2_city',
        'extID': 'extid',
        'ROAD_TYPE': 'road_type',
        'POINT_1_LNG': 'point_1_longitude',
        'REPORT_ID': 'id',  # primary key
        'POINT_1_COUNTRY': 'point_1_country',
        'DISTANCE_IN_METERS': 'distance_in_meters',
        'REPORT_NAME': 'report_name',
        'RBA_ID': 'rba_id',
        '_id': 'source_id',
    }

    @classmethod
    def fromdict(cls, d):
        renamed_d = dict([(cls.raw_data_column_map[k], v) for k,v in d.items()])
        return cls(**renamed_d)

class ParkingLot(Base):

    __tablename__ = "parking_lots"

    # Column definitions
    garage_code = Column(String, primary_key=True)
    city = Column(String)
    postal_code = Column(Integer)
    street = Column(String)
    house_number = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)

    @classmethod
    def fromdict(cls, d):
        d['garage_code'] = d.pop('garagecode')
        d['postal_code'] = d.pop('postalcode')
        d['house_number'] = d.pop('housenumber')
        return cls(**d)

metadata_registry = {
    'Traffic Sensor': TrafficSensor,
    'Parking Lot': ParkingLot,
}

# Reference Data Models

class DataType(Base):

    __tablename__ = "data_types"

    # Column definitions
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)

    @classmethod
    def get_or_create(cls, name, session):
        instance = session.query(cls).filter_by(name=name).first()
        if instance:
            return instance
        else:
            instance = cls(name=name)
            session.add(instance)
            session.flush()
            session.commit()
            return instance


class Location(Base):

    __tablename__ = "locations"

    # Column definitions
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)

    @classmethod
    def get_or_create(cls, name, session):
        instance = session.query(cls).filter_by(name=name).first()
        if instance:
            return instance
        else:
            instance = cls(name=name)
            session.add(instance)
            session.flush()
            session.commit()
            return instance


class Dataset(Base):
    """Dataset"""

    __tablename__ = "datasets"

    # Column definitions
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    url = Column(String, unique=True)
    data_type_id = Column(Integer, ForeignKey('data_types.id'))
    location_id = Column(Integer, ForeignKey('locations.id'))

    @classmethod
    def fromdict(cls, d):
        return cls(
            name = d['name'],
            data_type_id = d['data_type_id'],
            url = d['url'],
            location_id = d['location_id'],
        )

    @property
    def raw_data_file_name(self):
        # Assumes all datasets have unique names
        return url_to_filename(self.url)

    def get_data_type_model_cls(self, session):
        data_type_name = session.query(DataType).filter_by(id=self.data_type_id).one().name
        return data_type_registry[data_type_name]

    @classmethod
    def get_or_create(cls, ds_dict, session):
        instance = session.query(cls).filter_by(name=ds_dict['name']).first()
        if instance:
            log.debug(f"Dataset '{ds_dict['name']}' already exists, using existing")
            return instance
        else:
            instance = cls.fromdict(ds_dict)
            session.add(instance)
            session.flush()
            session.commit()
            return instance


reference_registry = {
    'Data Type': DataType,
    'Location': Location,
    'Dataset': Dataset,
}


def create_tables():
    for t in metadata_registry.values():
        t.__table__.create(bind=db_engine, checkfirst=True)
    for t in reference_registry.values():
        t.__table__.create(bind=db_engine, checkfirst=True)
    for t in data_type_registry.values():
        t.__table__.create(bind=db_engine, checkfirst=True)
