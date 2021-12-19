# CityPulse Smart City Data ETL

Author: Sam Patterson

Date: 19.12.2012

## Introduction

The purpose of this document is to describe the steps taken to build an extract, transform, load (ETL)[^etl-wiki] pipeline for Smart City data collected from the City Pulse[^citypulse-datasets] dataset.

## Explore and assess the data

### Datasets

The following data is listed on the CityPulse website.

| Data type | City | Raw format | # Datasets listed |
| --- | --- | --- | --- |
| Road Traffic Data | Aarhus | Compressed CSV files | 4 |
| Pollution Data | Aarhus, Brasov | Compressed CSV files | 2 |
| Weather Data	 | Aarhus, Brasov | Compressed JSON files | 4 |
| Parking Data | Aarhus | CSV file | 2 |
| Social (Webcasted) Event Data | Surrey | CSV file | 1 |
| Cultural Event Data | Aarhus | CSV file | 1 |
| Library Event Data | Aarhus | CSV file | 1 |

To being with, the 'raw' format of each dataset was manually downloaded. However, several of the files were found to be duplicated and mis-categorised.

### Dataset index issues

Upon initial review of the linked datasets, the following issues were identified:

- Linked file for the 'Aarhus Road Traffic Dataset-1' ([`citypulse_traffic_raw_data_surrey_feb_jun_2014.tar.gz`](http://iot.ee.surrey.ac.uk:8080/datasets/traffic/traffic_feb_june/citypulse_traffic_raw_data_surrey_feb_jun_2014.tar.gz)) indicates that it is from Surry.
- Linked file for the 'Aarhus Road Traffic Dataset-4' ([`cultural_events_aarhus.csv`](http://iot.ee.surrey.ac.uk:8080/datasets/aarhusculturalevents/cultural_events_aarhus.csv)) points to the 'Aarhus Cultural Event Dataset-1'.
- Linked file for the 'Brasov Pollution Dataset-1', 'Brasov Weather Dataset-1/2' files ([`citypulse_pollution_annotated_data_aarhus_aug_oct_2014.tar.gz`](http://iot.ee.surrey.ac.uk:8080/datasets/pollution/citypulse_pollution_annotated_data_aarhus_aug_oct_2014.tar.gz) and [`raw_weather_data_aarhus.tar.gz`](http://iot.ee.surrey.ac.uk:8080/datasets/weather/feb_jun_2014/raw_weather_data_aarhus.tar.gz) and [`raw_weather_data_aug_sep_2014.zip`](http://iot.ee.surrey.ac.uk:8080/datasets/weather/aug_sep_2014/raw_weather_data_aug_sep_2014.zip)) point to the same files from Aarhus.

An inspection into the backend file structure of the website[^citypulse-backend] was conducted, however the correct files did not appear to be there either. In these cases, the duplicated files are being ignored from here on and in the developed tool.

Further information about each data type's raw format can be found in the appendix.

### Extracting the data

To inspect the data more closely and begin building the initial 'extract' step of the ETL pipeline. Functions have been set up to automatically download and extract, where necessary, the datasets based on a JSON configuration file with the following format.

```json
[
    {
        "name": "Aarhus Road Traffic Dataset-1",
        "type": "Road Traffic Data",
        "url": "http://iot.ee.surrey.ac.uk:8080/datasets/traffic/traffic_feb_june/citypulse_traffic_raw_data_surrey_feb_jun_2014.tar.gz",
        "location": "Aarhus"
    },
    {
        "name": "Surrey Social Event Dataset-1",
        "type": "Social Event Data",
        "url": "http://iot.ee.surrey.ac.uk:8080/datasets/surreyevents/surrey_events.csv",
        "location": "Surrey"
    },
    ...
]
```

From here, each 

 There is a variety of different formats across the datasets.


The following extraction steps have been developed as part of the pipeline.

- Unpacked either `.tar.gz` or `.zip` files
- Applied header names to CSVs where (based on other similar files)
- Converted to pandas DataFrame objects[^pandas]


## Define the data model

## Run ETL to model the data

- Missing weather data samples inj `null` values

## Discuss future scenarios

## Bonus questions

## Limitations and future work

- Find out where the wrongly linked datasets are
- Use blob storage for raw data? (e.g. Azure Blog / AWS S3)

## Summary

## Appendix

### Raw data format

#### Road Traffic Data

These files were compressed as either `.tar.gz` or `.zip` files so needed to be uncompressed. The unpacked files are all CSVs with the following columns:

- `status`
- `avgMeasuredTime`
- `avgSpeed`
- `extID`
- `medianMeasuredTime`
- `TIMESTAMP`
- `vehicleCount`
- `_id`
- `REPORT_ID`

Some CSVs were missing the column headers, so it 

#### Pollution Data

#### Weather Data

#### Parking Data

#### Social (Webcasted) Event Data

#### Cultural Event Data

#### Library Event Data

[^etl-wiki]: https://en.wikipedia.org/wiki/Extract,_transform,_load
[^citypulse-datasets]: http://iot.ee.surrey.ac.uk:8080/datasets.html
[^citypulse-backend]: http://iot.ee.surrey.ac.uk:8080/datasets
[^pandas]: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html
