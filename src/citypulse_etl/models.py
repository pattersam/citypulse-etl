"""Data model classes"""

import os
import json

from sqlalchemy import Column, Integer, String, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base

from .database import db_engine

import logging
log = logging.getLogger(__name__)

Base = declarative_base()


# Primary Data Models

# XXX TODO

# Meta Data Models

class TrafficSensor(Base):

    __tablename__ = "traffic_sensors"

    # Column definitions
    id = Column(Integer, primary_key=True)
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
    point_1_country = Column(String)
    distance_in_meters = Column(Float)
    report_name = Column(String, unique=True)
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
        'POINT_1_LNG': 'point_2_longitude',
        'REPORT_ID': 'report_id',
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
    id = Column(Integer, primary_key=True)
    garagecode = Column(String)
    city = Column(String)
    postalcode = Column(Integer)
    street = Column(String)
    housenumber = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)

    @classmethod
    def fromdict(cls, d):
        return cls(**d)

metadata_registry = {
    'Traffic Sensor': TrafficSensor,
    'Parking Lot': ParkingLot,
}

# Reference Data Models

class Dataset(Base):
    """Dataset"""

    __tablename__ = "datasets"

    # Column definitions
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    data_type = Column(String)
    url = Column(String, unique=True)
    location_id = Column(Integer, ForeignKey('locations.id'))

    @classmethod
    def fromdict(cls, d):
        return cls(
            name = d['name'],
            data_type = d['type'],
            url = d['url'],
            location_id = d['location_id'],
        )

    @property
    def raw_data_file_name(self):
        # Assumes all datasets have unique names
        return self.url.split('/')[-1]

    @property
    def data_type_cls(self):
        return data_type_registry[self.data_type]

class Location(Base):

    __tablename__ = "locations"

    # Column definitions
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)


def create_tables():
    for t in metadata_registry.values():
        t.__table__.create(bind=db_engine, checkfirst=True)
    Dataset.__table__.create(bind=db_engine, checkfirst=True)
    Location.__table__.create(bind=db_engine, checkfirst=True)
