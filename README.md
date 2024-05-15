# Courier Delivery Snowflake Data Dimensional Modelling
## Overview

This project provides a Python Snowflake data dimensional modelling for modeling courier delivery data into datawarehouse. It can be used to ingest data from a CSV file, create and populate dimension and fact tables in a snowflake schema database (PostgreSQL in this example), and execute user-defined SQL queries for further analysis.

## Features

- Data transformation and cleaning for courier delivery data.
- Creation of dimension and fact tables in a star schema using SQLAlchemy.
- Population of database tables with processed data.
- Execution of SQL queries for data analysis.

## Project Structure
The project is organized into the following directories and files:

* **config/**
    * `config.example`: This file stores dummy database credentials (username, password, host) and other configurations.

* **database/**
    * `create_table.py`: This file contains functions for creating database tables using SQLAlchemy.
    * `db_connect.py`: This file defines a function to establish a connection to the PostgreSQL database.
    * `db_query.py`: This file provides functionality to execute SQL queries on the database.
    * `populate_db_table.py`: This file contains functions for populating database tables with processed data.

* **dataload/**     
    * `date_convert.py`: This file contains functions converting string to date format and extracting month, year, quarter.
    * `tables_data.py`: This file defines a functions generate different database table data.
    * `unique_subset.py`: This file provides functionality to subset a dataframe based on unique sets of values in a column.

* **requirements.txt**: This file lists the Python dependencies required for the project.

* **main.py**: This script serves as the entry point for the data pipeline. It orchestrates data processing, database operations, and query execution.

## Usage

 1. Install project dependencies: `pip install -r requirements.txt`
 2. Configure database credentials in `config/config.example`.
 3. Place your CSV data file in the same directory as the project (or adjust the path in `main.py`).
 4. Run the script: `python main.py data_path.csv` (replace `data_path.csv` with your actual file path).

## Configuration

Database credentials (username, password, host) are stored in the `config/config.py file`. Update these values according to your database setup.

## Dependencies 

The specific dependencies required for this project will be listed in the `requirements.txt` file. You can install them using the command mentioned in the Usage section.

## Contributing

We welcome contributions to improve this project. Please create a pull request on GitHub with your changes and adhere to the project coding style and documentation standards.

## License

This project is licensed under the MIT License. See the LICENSE file for details.