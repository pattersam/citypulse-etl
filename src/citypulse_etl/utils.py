"""Common utility functions and constants"""

import os
import requests

from dotenv import load_dotenv
load_dotenv()

RAW_DATA_DIR = os.getenv('RAW_DATA_DIR')
if not os.path.exists(RAW_DATA_DIR):
    os.makedirs(RAW_DATA_DIR)

def url_to_filename(url: str):
    return url.split('/')[-1]

def download_file(url: str, fname: str):
    r = requests.get(url)
    fpath = os.path.join(RAW_DATA_DIR, fname)
    with open(fpath, 'wb') as fp:
        fp.write(r.content)

def check_for_header(fname):
    with open(fname, encoding='utf-8') as f:
        first_line = f.readline().strip()
        return all([s.strip().isidentifier() for s in first_line.split(',')])
