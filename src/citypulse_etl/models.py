"""Data model classes"""

import os
import json

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

from .database import db_engine

import logging
log = logging.getLogger(__name__)

Base = declarative_base()


# Primary Data Models

# XXX TODO

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
    Dataset.__table__.create(bind=db_engine, checkfirst=True)
    Location.__table__.create(bind=db_engine, checkfirst=True)
