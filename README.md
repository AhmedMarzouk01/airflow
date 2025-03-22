#COVID-19 Data Pipeline with Apache Airflow 🚀
#Automated Data Processing Pipeline for COVID-19 Case Data
#Extract | Transform | Load | Analyze

📌 Project Overview
The COVID-19 Data Pipeline is an Apache Airflow-powered ETL workflow that automates the process of fetching, cleaning, and loading global COVID-19 case data from Johns Hopkins University (JHU) into a Microsoft SQL Server database for further analysis.

This project demonstrates how to efficiently process over 4 million rows of pandemic data, handling missing values, data inconsistencies, and database optimization while ensuring the dataset is structured for analytical use.

⚡ Key Features:
✅ Automated Data Fetching: Downloads JHU’s daily COVID-19 reports directly from GitHub.
✅ Scalable ETL Pipeline: Processes large datasets efficiently using Airflow’s DAGs.
✅ Data Cleaning & Transformation: Standardizes country names, removes null values, and ensures consistency.
✅ Database Optimization: Implements dimensional modeling for efficient querying.
✅ Robust Error Handling: Skips missing files, logs errors, and maintains data integrity.

📊 Project Architecture
🔁 Workflow of the Airflow DAG
pgsql

            +--------------------------+
            | Start DAG Execution       |
            +-----------+--------------+
                        |
                        v
            +--------------------------+
            | Create Database Schema    |
            | (Tables & Constraints)    |
            +-----------+--------------+
                        |
                        v
            +--------------------------+
            | Fetch Data from GitHub    |
            +-----------+--------------+
                        |
                        v
            +--------------------------+
            | Clean & Transform Data    |
            | (Standardize, Handle NULLs)|
            +-----------+--------------+
                        |
                        v
            +--------------------------+
            | Load into SQL Server      |
            +-----------+--------------+
                        |
                        v
            +--------------------------+
            | Update Facts Table        |
            | (Ensure Data Consistency) |
            +-----------+--------------+
                        |
                        v
            +--------------------------+
            | DAG Execution Complete    |
            +--------------------------+


#🛠️ Tech Stack & Tools
Category	Technology Used
Orchestration	Apache Airflow
Data Extraction	Python (requests, pandas)
Database	Microsoft SQL Server
Database ORM	SQLAlchemy
Programming Language	Python
Task Scheduling	Airflow DAGs



│
📝 Detailed ETL Process
1️⃣ Database Schema Creation (SCHEMA_SQL)
Creates three tables:

dim_date → Stores unique dates

dim_location → Stores unique locations with latitude & longitude

fact_covid_cases → Stores daily case counts

2️⃣ Data Extraction (fetch_data)
Pulls the latest COVID-19 reports from JHU's GitHub repository.

Handles missing files and skips execution when necessary.

Fetches & processes over 4 million rows of historical pandemic data.

3️⃣ Data Transformation (clean_transform_data)
Standardizes column names for consistency.

Handles missing values & incorrect data types.

Maps country names for consistency in reports.

4️⃣ Load Dimension Tables (load_dimensions)
Date dimension (dim_date) → Inserts unique date values.

Location dimension (dim_location) → Handles duplicates and missing coordinates.

5️⃣ Load Fact Table (load_facts)
Ensures data integrity before inserting records into fact_covid_cases.

Updates existing records if new case counts are reported.

