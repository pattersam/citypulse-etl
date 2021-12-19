"""Main `citypulse-etl` command for running the ETL pipeline"""

import json
import os
import shutil

from typing import Dict, List

from citypulse_etl import pipeline, models

import logging
log = logging.getLogger(__name__)

import argparse
parser = argparse.ArgumentParser(description='Run the City Pulse ETL Pipeilne.')
parser.add_argument('tasks', nargs='*', type=str,
                    help='task(s) to perform, i.e. `clean-db` / `init-metadata` / `clean-raw-files` / `run-pipeline`')
parser.add_argument('--dataset-json', type=str, help='json file of datasets to process')
parser.add_argument('--skip-download', action='store_true', default=False,
                    help='skip downloading files')

def clear_database():
    db_file = os.getenv('SQLITE_DB_FILE')
    if os.path.exists(db_file): os.remove(db_file)
    open(db_file, 'a').close()
    log.info(f"Database cleared.")

def init_database(clear_first=True):
    if clear_first: clear_database()
    models.create_tables()
    log.info(f"Database initialised.")

def init_metadata():
    # XXX TODO
    log.info(f"Metadata initialised.")

def clear_raw_data_files():
    raw_data_dir = os.getenv('RAW_DATA_DIR')
    if os.path.exists(raw_data_dir):
        shutil.rmtree(raw_data_dir)
    os.makedirs(raw_data_dir)
    log.info(f'Raw data cleared.')

def run_pipelines(dataset_dicts: List[Dict], skip_download: bool = False):
    for ds_dict in dataset_dicts:
        log.info(f"Running pipeline for dataset: {ds_dict['name']}")
        pipeline.run_pipeline(
            ds_dict,
            skip_download=skip_download,
            )

def main():
    args = parser.parse_args()
    if len(args.tasks) == 0:
        log.error(f"No tasks provided.")
    for task in args.tasks:
        if task == 'clean-raw-files':
            clear_raw_data_files()
        elif task == 'clean-db':
            init_database(clear_first=True)
        elif task == 'init-metadata':
            init_metadata()
        elif task == 'run-pipeline':
            if args.dataset_json is None:
                log.error(f"--dataset-json option require to run pipeline")
                continue
            dataset_dicts = json.load(open(args.dataset_json))
            run_pipelines(dataset_dicts, skip_download=args.skip_download)
        else:
            log.error(f"Unknown task: {task}")
