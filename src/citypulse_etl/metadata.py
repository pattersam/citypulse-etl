"""Functions for initialising metadata"""

import os
import pandas as pd

from .database import Session
from .models import metadata_registry
from .utils import RAW_DATA_DIR, download_file, url_to_filename

import logging
log = logging.getLogger(__name__)

def initialise_metadata(md_dict):
    session = Session()

    url = md_dict['url']
    fname = url_to_filename(url)
    md_cls = metadata_registry[md_dict['name']]

    log.info("Downloading the metadata...")
    download_file(url, fname)

    log.info("Read in metadata file...")
    df = pd.read_csv(os.path.join(RAW_DATA_DIR, fname))
    for r in df.to_dict(orient='records'):
        md_record = md_cls.fromdict(r)
        session.add(md_record)

    session.commit()
    session.close()


