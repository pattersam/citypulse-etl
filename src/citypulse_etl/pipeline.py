"""Functions for extracting data from raw sources"""

import os
import requests
import tarfile
import zipfile
import pandas as pd

from sqlalchemy.exc import IntegrityError
from typing import Dict, List

from .database import Session
from .models import Dataset, DataType, Location
from .utils import RAW_DATA_DIR, download_file

import logging
log = logging.getLogger(__name__)

def unpack_raw_data_file(fname: str, warn_on_overwrite: bool = False):
    if fname.endswith('.tar.gz'):
        with tarfile.open(os.path.join(RAW_DATA_DIR, fname), 'r:gz') as tar:
            for m in tar.getmembers():
                fname = os.path.join(RAW_DATA_DIR, m.path)
                if os.path.exists(fname) and warn_on_overwrite:
                    log.warn(f"{fname} already exists, overwriting")
            tar.extractall(RAW_DATA_DIR)
            return [m.path for m in tar.getmembers()]
    if fname.endswith('.zip'):
        with zipfile.ZipFile(os.path.join(RAW_DATA_DIR, fname), 'r') as zip:
            zip.extractall(RAW_DATA_DIR)
            return [m.filename for m in zip.filelist]
    else:
        raise ValueError(f'Unknown format for: {fname}')

def iter_dataset_files(fname: str) -> str:
    if not fname.endswith('.csv'):
        csv_files = unpack_raw_data_file(fname)
    else:
        csv_files = [fname]
    for fname in csv_files:
        if 'MACOSX' in fname: continue
        yield os.path.join(RAW_DATA_DIR, fname)

def insert_rows_from_df(df: pd.DataFrame, data_type_cls, session: Session):
    """Inserts rows for a `data_type` from a pandas DataFrame"""
    # Doing it this way instead of creating a `data_type_cls` object for all 
    # rows to improve performance.
    log.debug(f"Converting {len(df)} row DataFrame to list of dicts")
    records = df.to_dict(orient='records')
    log.debug(f"Performing insert in {data_type_cls.__tablename__}")
    session.execute(data_type_cls.__table__.insert(), records)

def run_pipeline(
    ds_dict: Dict,
    skip_download: bool = False,
    ):
    """Runs the ETL pipeline for a single dataset"""

    session = Session()

    log.info("Creating the dataset record (and location if required)...")
    data_type = DataType.get_or_create(ds_dict['data_type'], session)
    ds_dict['data_type_id'] = data_type.id  # foriegn key of data type
    location = Location.get_or_create(ds_dict['location'], session)
    ds_dict['location_id'] = location.id  # foriegn key of location
    dataset = Dataset.get_or_create(ds_dict, session)
    data_type_model_cls = dataset.get_data_type_model_cls(session)

    if not skip_download:
        log.info("Downloading raw dataset files...")
        download_file(dataset.url, dataset.raw_data_file_name)
    else:
        log.info("Using cached dataset files (skipping download)...")

    log.info("Unpacking / listing dataset files...")
    for fname in iter_dataset_files(dataset.raw_data_file_name):
        log.info(f"Reading {fname}...")
        raw_data = data_type_model_cls.read_raw_data(fname, dataset)
        log.debug(f"Validating raw data...")
        data_type_model_cls.validate_raw_data(raw_data)

        log.debug(f"Transforming {len(raw_data)} rows...")
        transformed_data = data_type_model_cls.transform_raw_data(raw_data, dataset)

        log.debug(f"Writing to {data_type_model_cls.__tablename__}...")
        try:
            insert_rows_from_df(transformed_data, data_type_model_cls, session)
        except IntegrityError:
            log.error("Uniqueness Constraint Failed, continuing without these rows...")

    session.commit()
    session.close()
