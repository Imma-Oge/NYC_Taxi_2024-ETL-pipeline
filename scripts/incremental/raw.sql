-------copy into table from stage with minimal transformation
COPY INTO bronze.raw_nyc_taxi
FROM (
    SELECT  
              
              $1:VendorID::integer,
              to_timestamp($1:tpep_pickup_datetime::string),
              to_timestamp($1:tpep_dropoff_datetime::string),
              $1:passenger_count::INTEGER,
              $1:trip_distance::FLOAT,
              $1:RatecodeID::INTEGER,
              $1:store_and_fwd_flag::VARCHAR(50),
              $1:PULocationID::INTEGER,
              $1:DOLocationID::INTEGER,
              $1:payment_type::INTEGER,
              $1:fare_amount::FLOAT,
              $1:extra::FLOAT,
              $1:mta_tax::FLOAT,
              $1:tip_amount::FLOAT,
              $1:tolls_amount::FLOAT,
              $1:improvement_surcharge::FLOAT,
              $1:total_amount::FLOAT,
              $1:congestion_surcharge::FLOAT,
              $1:Airport_fee::FLOAT,
              current_date AS last_updated_at           
     FROM @inc_load_dec_taxi
      );
     

     SELECT * FROM   bronze.raw_nyc_taxi 
     
     SELECT * FROM raw_stream

       
