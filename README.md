# CityPulse ETL

A Python extract, transform, load (ETL) pipeline for the [CityPulse](http://iot.ee.surrey.ac.uk:8080/datasets.html) Smart City dataset.

## Prerequisites

- Python 3 (3.8 has been used for development)

## Usage

Ensure the following environment variables are set, either with the `.env` file or in your environment.

- `RAW_DATA_DIR` - the directory where the raw data will be downloaded and extracted (e.g. `data/raw`)

You'll also need a json file with a list of dictionaries for each dataset.

To initialise your databaes, run:

```
citypulse-etl --metadata-json=metadata.json clean-db init-metadata
```

To run the ETL on a collection of datasets, run:

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

## Documentation

A report about this project is available under `docs/report.md`.

### Building as a pdf

To build a pdf out of the report, [pandoc](https://pandoc.org/) has be used along with [this helpful guide](https://learnbyexample.github.io/customizing-pandoc/)

#### Install Pandoc and filters

- Update apt-get: `sudo apt-get update -y`
- Install pandoc: `sudo apt-get install -y pandoc`
- Install pdflatext: `sudo apt-get install -y texlive-latex-base texlive-fonts-recommended texlive-fonts-extra texlive-latex-extra texlive-xetex librsvg2-bin texlive-science`
- Install filter for [mermaid charts](https://mermaid-js.github.io/mermaid/#/): `npm install --global mermaid-filter`
- Install schema generator: `npm install -g sqleton`
- Install graphviz (for scchema generator): `sudo apt-get install -y graphviz`
- Install pydeps: `pip install pydeps`

#### Building schema diagram

The [sqleton](https://github.com/inukshuk/sqleton) tool can build a scheme from the SQLite databse.

- Run: `sqleton -o docs/schema.svg -L circo data/database.db`

#### Building package dependecy graph

- Run: `pydeps --noshow -o docs/deps.svg -T svg src/citypulse_etl`

#### Building report

[Pandoc](https://pandoc.org/) is used to convert the `docs/report.md` Markdown file to a PDF

- First change into the docs directory: `cd docs`
- Then run: `./compile.sh`
