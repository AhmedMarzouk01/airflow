COVID-19 Analytics Pipeline Using Apache Airflow
Project Overview
This project automates the ETL process for COVID-19 case data from Johns Hopkins University's (JHU) GitHub repository. The pipeline extracts, transforms, and loads (ETL) data into a structured SQL Server database for further analysis. Apache Airflow orchestrates the workflow.

The pipeline successfully fetches and processes over 4 million rows of COVID-19 case data, ensuring structured and clean data storage.

Tech Stack
Orchestration: Apache Airflow

Data Extraction: requests (Python)

Data Processing: pandas

Database: Microsoft SQL Server

Database ORM: SQLAlchemy

Programming Language: Python

Project Structure
pgsql
Copy
Edit
Covid19-Airflow-Pipeline/
│-- dags/
│   ├── covid_analytics_dag.py  # Airflow DAG script
│-- sql/
│   ├── schema.sql  # SQL schema for the database
│-- config/
│   ├── airflow_settings.py  # Airflow configurations
│-- README.md  # Project documentation
│-- requirements.txt  # Required Python packages
│-- .gitignore  # Git ignored files
Pipeline Workflow
The DAG executes the following steps:

Database Schema Creation (SCHEMA_SQL)

Creates three tables: dim_date, dim_location, and fact_covid_cases if they do not already exist.

Extract Data (fetch_data)

Downloads the COVID-19 daily reports from JHU's GitHub repository.

Handles missing files by skipping execution on unavailable dates.

Processes over 4 million rows across historical datasets.

Transform Data (clean_transform_data)

Standardizes column names.

Handles missing values and incorrect data formats.

Maps country names to ensure consistency.

Load Dimension Tables (load_dimensions)

Inserts unique date values into dim_date.

Inserts unique locations into dim_location, handling duplicates and missing coordinates.

Load Fact Table (load_facts)

Joins data with dim_date and dim_location.

Inserts new records into fact_covid_cases.

Updates existing records if case counts have changed.


