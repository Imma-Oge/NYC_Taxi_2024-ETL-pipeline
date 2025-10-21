

create or replace database incremental_nyc_2024

create or replace schema bronze;

create or replace schema silver;

create or replace schema gold;
use schema bronze

CREATE OR REPLACE FILE FORMAT parquett
    TYPE = PARQUET;

CREATE OR REPLACE STAGE inc_load_dec_taxi
FILE_FORMAT = parquett;


---creating nyc taxi table using the infer schema method
list '@inc_load_dec_taxi'

-- Query the INFER_SCHEMA function.
 SELECT *
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION=> '@inc_load_dec_taxi', 
       FILE_FORMAT=>'parquett'
      )
    );

        ---create table with the inferred schema 
         
    CREATE OR REPLACE TABLE raw_inc_yellow_tripdate_2024
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION => '@inc_load_dec_taxi', 
      FILE_FORMAT => 'parquett'
    )
  )
);

---updating the raw table to add timestamp
 
 ALTER TABLE raw_inc_yellow_tripdate_2024 ADD COLUMN last_updated_at TIMESTAMP_NTZ;

UPDATE raw_inc_yellow_tripdate_2024  SET last_updated_at = CURRENT_TIMESTAMP();

---creating stream to track changes(update, insert, delete) in raw table
    CREATE OR REPLACE STREAM raw_stream_2024 ON TABLE raw_inc_yellow_tripdate_2024

    select * from raw_stream_2024  

 -------creating the transformed layer in silver schema

 CREATE OR REPLACE TABLE silver.transf_yellow_tripdate_2024
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
    
	
    
