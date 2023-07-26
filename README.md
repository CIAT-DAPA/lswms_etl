# EWP ETL

![GitHub release (latest by date)](https://img.shields.io/github/v/release/CIAT-DAPA/lswms_etl) ![](https://img.shields.io/github/v/tag/CIAT-DAPA/lswms_etl)

**Important notes**

These Scripts must be used in conjunction with the models that was developed for the project, which you can find in this [repository](https://github.com/CIAT-DAPA/lswms_models).
## Features

- Built using Mongoengine for MongoDB
- Supports Python 3.x

## Getting Started

To use these Models, it is necessary to have a MongoDB instance running and be connected to the postgres database where the water points are stored..

### Prerequisites

- Python 3.x
- MongoDB
- [modelswp](https://github.com/CIAT-DAPA/lswms_models)

### Project Structure

- `conf/`: Folder where the database credentials will be saved.
- `error/`: Folder where all errors will be saved, (it is not necessary to create it, it is created automatically when an error is found).
- `imports/`: This folder contains the Python scripts used for data import.
- `Data` : This folder contains a shapefile with the administrative levels of ethiopia, this file is used to import the administrative levels in the database, it also contains a csv file with the water points to extract from the postgres database and the watershed to which they belong, and finally a csv file with the water point profiles. 


## Instalation

To use ETL we must install a set of requirements, which are in a text file, for this process we recommend to create a virtual environment, this in order not to install these requirements in the entire operating system.

1. Clone the repository
````sh
git clone https://github.com/CIAT-DAPA/lswms_etl.git
````

2. Create a virtual environment
````sh
python -m venv env
````

3. Activate the virtual environment
- Linux
````sh
source env/bin/activate
````
- windows
````sh
env\Scripts\activate.bat
````

4. Install the required packages

````sh
pip install -r requirements.txt
````

## config file example


The project uses a configuration file `config.conf` to adjust certain parameters. An example of this file is located inside the project in the conf folder.


## Scripts import summary


### import_adm1

This script will traverse the shapefile converted to a dataset and extract the following fields:

- name: `str` Name of the Adm1(Zone).
- ext_id: `str` external id from the Adm1(Zone).

### import_adm2

This script will traverse the shapefile converted to a dataset and extract the following fields:

- name: `str` Name of the Adm2(Woreda).
- ext_id: `str` external id from the Adm2(Woreda).

### import_adm3

This script will traverse the shapefile converted to a dataset and extract the following fields:

- name: `str` Name of the Adm3(Kebele).
- ext_id: `str` external id from the Adm3(Kebele).

### import_waterpoint

This script will traverse the database in postgres converted to a dataset and extract the following fields:

- name: `str` Name of the waterpoint.
- lat: `float` latitude of the waterpoint.
- lon: `float` longitude of the waterpoint.
- area: `float` area of the waterpoint.
- ext_id: `str` external id from the waterpoint.

### import_watershed

This script joins a csv file and merges it with the dataset containing the waterpoints and extracts the watershed to the kebele that belongs to it.:

- name: `str` Name of the watershed.
- area: `float` area of the watershed.

## import climatology

this script goes through the postgres database and saves the climatology history of each water point.

## import moinitored_data
this script goes through the postgres database and saves the monitored data in an entity called monitored, this data corresponds to the daily information of all the variables contained in each water point.
