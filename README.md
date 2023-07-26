# EWP ETL

![GitHub release (latest by date)](https://img.shields.io/github/v/release/CIAT-DAPA/lswms_models) ![](https://img.shields.io/github/v/tag/CIAT-DAPA/lswms_models)

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
- [modelswp](https://github.com/CIAT-DAPA/spcat_orm)

### Project Structure

- `conf/`: Folder where the database credentials will be saved.
- `error/`: Folder where all errors will be saved, (it is not necessary to create it, it is created automatically when an error is found).
- `imports/`: This folder contains the Python scripts used for data import.
- `Data` : This folder contains a shapefile with the administrative levels of ethiopia, this file is used to import the administrative levels in the database, it also contains a csv file with the water points to extract from the postgres database and the watershed to which they belong, and finally a csv file with the water point profiles. 


## Visual folder structure

Main Directory: MyProject
├── conf
├── data
│   ├── adminlevel
│   │   ├── shapefile1.shp
│   │   ├── shapefile1.shx
│   │   ├── shapefile1.dbf
│   │   ├── shapefile2.shp
│   │   ├── shapefile2.shx
│   │   ├── shapefile2.dbf
│   │   ├── shapefile3.shp
│   │   ├── shapefile3.shx
│   │   ├── shapefile3.dbf
│   │   ├── shapefile4.shp
│   │   ├── shapefile4.shx
│   │   └── shapefile4.dbf
│   ├── profile.xlsx
│   └── whade.xlsx
├── error
├── parameters
├── imports
└── tests

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


The project uses a configuration file `config.conf` to adjust certain parameters. Below is an example of how the `config.conf` file looks like:


# Application Configuration

this is an example of the configuration file you should create, with the parameters you are going to use to connect to the database

[db_conf]
mongo_user = mongouser
mongo_pass = mongopass
mongo_port = mongoport 
mongo_db_host = mongo host
mongo_db_name = mongodbname
postgres_user = postgresuser
postgres_pass = postgrespass
postgres_port = postgresport
postgres_db_host = postgreshost
potgres_db_name = postgresdatabasename


## Scripts import summary


### import_adm1

Scripts to import the admin level one in the database.

