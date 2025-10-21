# NYC_Taxi_2024-ETL-pipeline


## Overview
NYC_Taxi_2024-ETL-pipeline is a comprehensive project aimed at demonstrating the Extract, Transform, Load (ETL) processes using real-world New York City Yellow Taxi trip data. This project showcases two fundamental data loading strategies; full load and incremental load, illustrating how data moves through different stages of a modern data pipeline.
All ETL operations are implemented purely in SQL.

## About the Dataset
The NYC Yellow Taxi dataset contains detailed trip records from yellow taxi cabs operating in New York City, including:

* Pickup and drop-off timestamps and locations
* Trip distances and fares
* Payment methods and tip amounts
* Passenger counts

This rich dataset enables analysis of urban mobility patterns, peak travel times, fare trends, and geographic demand distribution across NYC's boroughs.
What This Project Demonstrates
This repository provides a step-by-step walkthrough of implementing:

Full Load Strategy: Complete data refresh where the entire dataset is extracted and loaded into the target system
Incremental Load Strategy: Efficient loading of only new or modified records, optimizing for performance and resource usage

Each approach is documented with clear explanations, practical scripts, and sample datasets organized in their respective directories.

## Architecture diagram 

<img width="581" height="293" alt="ETL architecture" src="https://github.com/user-attachments/assets/a98036da-43d9-4555-b0bf-42e77c3c339c" />
