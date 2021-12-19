# CityPulse ETL

A Python extract, transform, load (ETL) pipeline for the [CityPulse](http://iot.ee.surrey.ac.uk:8080/datasets.html) Smart City dataset.

## Prerequisites

- Python 3 (3.9 has been used for development)

## Usage

Ensure the following environment variables are set, either with the `.env` file or in your environment.

- `RAW_DATA_DIR` - the directory where the raw data will be downloaded and extracted (e.g. `data/raw`)

You'll also need a json file with a list of dictionaries for each dataset.

To run the pipeline, use:

```
citypulse-etl --dataset-json=all-datasets.json run-pipeline
```

## Development

### Set up

- Create a virtual environment using `python -m virtualenv venv`
- Activate the virtual environment using `venv\Scripts\activate` on Windows or `source venv/bin/activate` on Linux/OSX
- Install requirements with `pip install -r requirements.txt`

## Assumptions and Limitations

- ...
