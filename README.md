# NYC_Taxi_2024-ETL-pipeline



## Overview
NYC_Taxi_2024-ELT-Pipeline is a comprehensive project designed to demonstrate the Extract, Transform, Load (ELT) process using real-world New York City Yellow Taxi Trip Data.

This project showcases two key data loading strategies — Full Load and Incremental Load, to illustrate how data moves through different stages of a modern data pipeline.

All ELT operations are implemented purely in SQL to highlight how core ELT principles can be achieved without external frameworks.



## Project Objective

The goal of this project is to demonstrate how to design and implement a complete ETL pipeline that efficiently handles raw transportation data and prepares it for analysis.

Key Deliverables

*  Implement Full Load and Incremental Load strategies for optimized data ingestion.

* Design an end-to-end SQL-driven pipeline for extract, transform, and load operations.

* Demonstrate performance optimization and data quality handling within SQL.



## Project Architecture

The architecture represents a modern data pipeline that moves data from raw source files through transformation stages into the analytical layer.

<img width="568" height="316" alt="Image" src="https://github.com/user-attachments/assets/7f7508f3-d6ff-45ff-8967-962b6809ed50" />



## Process Overview

Extract – Load NYC taxi trip data from Parquet files.

Transform – Clean, standardize, and prepare data for analytics.

Load – Insert data into target tables using both full and incremental load strategies.



## Dataset

The dataset used is the NYC Yellow Taxi Trip Data, which contains detailed information about taxi trips in New York City.

Dataset Features

* Pickup and drop-off timestamps and locations

* Trip distances and fare amounts

* Payment methods and tip amounts

* Passenger counts

These fields enable exploration of urban mobility patterns, peak hours, fare trends, and geographic demand across the city’s boroughs.

### Sample Columns
| Column	| Description
|----------------------|-------------------------
| tpep_pickup_datetime | Trip start date and time
| tpep_dropoff_datetime | Trip end date and time
| passenger_count	     | Number of passengers per trip
| trip_distance	     | Distance covered during the trip
| payment_type	        | Payment method used
| fare_amount	        | Base fare for the trip
| tip_amount	        | Tip provided by the customer
| total_amount	        | Final billed amount



## What This Project Demonstrates

This repository provides a hands-on demonstration of:

* Full Load Strategy: Complete data refresh where the entire dataset is reloaded each cycle.

* Incremental Load Strategy: Load only new or modified records to improve performance and reduce resource consumption.

* Data Quality Handling: Ensuring clean, consistent data during transformation.

* SQL Optimization: Efficient query design for large-scale datasets.

Each method includes documentation, practical SQL scripts, and sample datasets organized in their respective directories.



## Tools & Technologies

| Tool |	Purpose
|-------
| SQL	| Core ETL implementation and data transformation
| Data Warehouse(Snowflake) | Target storage for processed data
| Parquet Files	| Raw data storage and ingestion format.



## Projecct Structure
```

NYC_Taxi_2024-ETL-Pipeline/
│
├── data/                 # Sample or raw datasets
├── full_load/            # SQL scripts for full load process
├── incremental_load/     # SQL scripts for incremental load process
├── docs/                 # Architecture diagrams
├── README.md             # Project overview
         
```



## How to Run the Project

* Clone this repository:
git clone https://github.com/Imma-Oge/NYC_Taxi_2024-ETL-pipeline.git

* Load sample data into your SQL environment or data warehouse.

* Run the scripts in the full_load or incremental_load directories, depending on your loading strategy.

* Verify results in your analytical tables or views.



## Insights & Results

* Identified peak travel hours and popular pickup zones across NYC.

* Analyzed fare patterns, tip behavior, and trip distance distributions.

* Compared efficiency of full vs. incremental loads for large data volumes.



## Learnings & Next Steps
Key Learnings

* Mastered the difference between full and incremental ETL strategies.

* Improved understanding of SQL-based data pipeline design.

* Gained hands-on experience in data validation and pipeline optimization.

### Future Improvements

* Add orchestration with tools like Airflow or Mage.

* Add Pipeline Health Monitoring and Alerts

* Integrate with cloud storage (e.g., GCS or S3).

* Add visualization layer (Power BI).



## Author

Immaculate Okoro
Data Engineer passionate about building reliable, scalable, and insight-driven data pipelines.
🔗 Connect with me on [LinkedIn](www.linkedin.com/in/immaculateogechi)






