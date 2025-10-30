
CREATE OR REPLACE DATABASE incremental_nyc_2024

USE DATABASE incremental_nyc_2024

CREATE OR REPLACE SCHEMA bronze;

CREATE OR REPLACE SCHEMA silver;

CEATE OR REPLACE SCHEMA gold;

USE SCHEMA  bronze
--to create a parquet file format 
CREATE OR REPLACE FILE FORMAT parquett
    TYPE = PARQUET;
    
--to create an internal stage for loading the parquet files
CREATE OR REPLACE STAGE inc_load_dec_taxi

FILE_FORMAT = parquett;



---understanding stage construct using the infer schema 
list '@inc_load_dec_taxi'

-- Query the INFER_SCHEMA function.
 SELECT *
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION=> '@inc_load_dec_taxi', 
       FILE_FORMAT=>'parquett'
      )
    );

------creating the raw table 

CREATE OR REPLACE TABLE bronze.raw_nyc_taxi 
(  

              VendorID integer,
              tpep_pickup_datetime TIMESTAMP,
              tpep_dropoff_datetime TIMESTAMP,
              passenger_count INTEGER,
              trip_distance FLOAT,
              RatecodeID INTEGER,
              store_and_fwd_flag VARCHAR(50),
              PULocationID INTEGER,
              DOLocationID INTEGER,
              payment_type INTEGER,
              fare_amount FLOAT,
              extra FLOAT,
              mta_tax FLOAT,
              tip_amount FLOAT,
              tolls_amount FLOAT,
              improvement_surcharge FLOAT,
              total_amount FLOAT,
              congestion_surcharge FLOAT,
              Airport_fee FLOAT,
              uploaded_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP() 
    	
	
);
---creating stream to track changes(insert) in the raw table

CREATE OR REPLACE STREAM raw_stream ON TABLE bronze.raw_nyc_taxi


 -------creating the transformed Table in the silver layer

 CREATE OR REPLACE TABLE silver.trans_nyc_taxi
(  

    vendor_id NUMBER(38,0),
    pickup_time timestamp,
	dropoff_time timestamp,
    months date,
    total_passengers NUMBER(38,0),
    trip_distance FLOAT,
    distance_group VARCHAR(50),
    ratecode_group VARCHAR(50),
    store_and_fwd_indication VARCHAR(50),
    taximeter_engaged_zone NUMBER(38,0),
    taximeter_disengaged_zone NUMBER(38,0),
    payment_type VARCHAR(50),
    fare_amount FLOAT,
    extra FLOAT,
    metered_rate_tax FLOAT,
    creditcard_tip_amount FLOAT,
    tolls_amount FLOAT,
	improvement_surcharge FLOAT, 
	total_amount_excluding_cash_tip FLOAT,
	congestion_surcharge FLOAT,
	LaGuardia_JFK_fee FLOAT,
    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() 
    	
