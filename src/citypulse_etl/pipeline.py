"""Functions for extracting data from raw sources"""

import os
import requests
import tarfile
import zipfile

from typing import Dict, List

from dotenv import load_dotenv
load_dotenv()

import logging
log = logging.getLogger(__name__)


RAW_DATA_DIR = os.getenv('RAW_DATA_DIR')
if not os.path.exists(RAW_DATA_DIR):
    os.makedirs(RAW_DATA_DIR)

def download_raw_dataset(url: str, fname: str):
    r = requests.get(url)
    fpath = os.path.join(RAW_DATA_DIR, fname)
    with open(fpath, 'wb') as fp:
        fp.write(r.content)

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
        yield os.path.join(RAW_DATA_DIR, fname)

def run_pipeline(
    ds_dict: Dict,
    skip_download: bool = False,
    ):
    """Runs the ETL pipeline for a single dataset"""

    url = ds_dict['url']
    ds_fname = url.split('/')[-1]

    if not skip_download:
        log.info("Downloading raw dataset files...")
        download_raw_dataset(url, ds_fname)
    else:
        log.info("Using cached dataset files (skipping download)...")

    log.info("Unpacking / listing dataset files...")
    for fname in iter_dataset_files(ds_fname):
        log.info(f"Reading {fname}...")
