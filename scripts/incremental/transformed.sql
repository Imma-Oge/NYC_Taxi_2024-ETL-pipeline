---------inserting records from bronze taxi 2024-01 to silverlayer


----initial full load



 INSERT INTO silver.transf_yellow_tripdate_2024
 (
 SELECT 
        ROW_NUMBER() OVER(ORDER BY "tpep_pickup_datetime") as primary_key,
        "VendorID" as vendor_id,
        TO_TIMESTAMP("tpep_pickup_datetime"::VARCHAR) as pickup_time,
        TO_TIMESTAMP("tpep_dropoff_datetime"::VARCHAR) as dropoff_time,
        date_trunc(month,TO_TIMESTAMP("tpep_pickup_datetime"::VARCHAR)) as months,
        "passenger_count" as total_passengers,
        "trip_distance",
       CASE
            WHEN "trip_distance"  BETWEEN  0.01 AND 79721.73  THEN  'Extra shorT' 
            WHEN "trip_distance"  BETWEEN  80721.74 AND 159443.46  THEN 'Short' 
            WHEN "trip_distance"  BETWEEN  159443.47 AND 239165.19 THEN 'Medium' 
            WHEN "trip_distance"  BETWEEN  239165.20 AND 318886.92 THEN 'Long' 
            WHEN "trip_distance"  BETWEEN  318886.93 AND 398608.62 THEN 'Exteme long' 
        ELSE 'None'
     END AS distance_group,
         CASE    "RatecodeID"
        WHEN  1 THEN 'Standard rate'
        WHEN  2 THEN 'JFK'
        WHEN  3 THEN 'Newark'
        WHEN  4 THEN 'Nassau or Westchester'
        WHEN  5 THEN 'Negotiated fare'
        WHEN  6 THEN 'Group ride'
        WHEN  99 THEN NulL
    ELSE null
    END AS ratecode_group,
     CASE 
        WHEN "store_and_fwd_flag" ='N' THEN 'No'
        WHEN "store_and_fwd_flag" ='Y' THEN'Yes'
    ELSE 'Not indicated'
    END AS store_and_fwd_indication,
    "PULocationID" AS taximeter_engaged_zone,
    "DOLocationID" AS taximeter_disengaged_zone,
     CASE "payment_type"
        WHEN 0 THEN 'Flex Fare trip'
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
     ELSE 'Voided trip'
    END AS "payment_type", 
    "fare_amount",
    REPLACE("extra",'-','') as extra, 
    "mta_tax" AS metered_rate_tax,
    "tip_amount" AS creditcard_tip_amount,
    "tolls_amount",
    "improvement_surcharge",
    "total_amount" AS total_amount_excluding_cash_tip,
    "congestion_surcharge",
    "Airport_fee" AS LaGuardia_JFK_fee, 
    current_date AS last_updated_at

FROM raw_inc_yellow_tripdate_2024 
WHERE "VendorID" IN (1,2,6,7) AND "payment_type" IN(0,1,2,3,4,5,6)
);

select * from silver.transf_yellow_tripdate_2024


----loading susequent data from bronze to silver using merge and target

 CREATE OR REPLACE STREAM raw_stream_2024 ON TABLE raw_inc_yellow_tripdate_2024

    select * from raw_stream_2024  

-- Create a task that runs every minute and includes transformations
CREATE OR REPLACE TASK load_new_nyc_data
 SCHEDULE='1 MINUTE'
  WAREHOUSE = compute_wh

WHEN
  SYSTEM$STREAM_HAS_DATA(' raw_stream_2024')
AS
   INSERT INTO silver.transf_yellow_tripdate_2024
 (  primary_key,
    vendor_id,
    pickup_time, 
	dropoff_time,
    months, 
    total_passengers, 
    trip_distance, 
    distance_group, 
    ratecode_group,
    store_and_fwd_indication,
    taximeter_engaged_zone, 
    taximeter_disengaged_zone, 
    payment_type, 
    fare_amount,
    extra, 
    metered_rate_tax,
    creditcard_tip_amount, 
    tolls_amount, 
	improvement_surcharge,  
	total_amount_excluding_cash_tip,
	congestion_surcharge, 
	LaGuardia_JFK_fee, 
    last_updated_at
    )
 SELECT 
        ROW_NUMBER() OVER(ORDER BY "tpep_pickup_datetime") as primary_key,
        "VendorID" as vendor_id,
        TO_TIMESTAMP("tpep_pickup_datetime"::VARCHAR) as pickup_time,
        TO_TIMESTAMP("tpep_dropoff_datetime"::VARCHAR) as dropoff_time,
        date_trunc(month,TO_TIMESTAMP("tpep_pickup_datetime"::VARCHAR)) as months,
        "passenger_count" as total_passengers,
        "trip_distance",
       CASE
            WHEN "trip_distance"  BETWEEN  0.01 AND 79721.73  THEN  'Extra shorT' 
            WHEN "trip_distance"  BETWEEN  80721.74 AND 159443.46  THEN 'Short' 
            WHEN "trip_distance"  BETWEEN  159443.47 AND 239165.19 THEN 'Medium' 
            WHEN "trip_distance"  BETWEEN  239165.20 AND 318886.92 THEN 'Long' 
            WHEN "trip_distance"  BETWEEN  318886.93 AND 398608.62 THEN 'Exteme long' 
        ELSE 'None'
     END AS distance_group,
         CASE    "RatecodeID"
        WHEN  1 THEN 'Standard rate'
        WHEN  2 THEN 'JFK'
        WHEN  3 THEN 'Newark'
        WHEN  4 THEN 'Nassau or Westchester'
        WHEN  5 THEN 'Negotiated fare'
        WHEN  6 THEN 'Group ride'
        WHEN  99 THEN NulL
    ELSE null
    END AS ratecode_group,
     CASE 
        WHEN "store_and_fwd_flag" ='N' THEN 'No'
        WHEN "store_and_fwd_flag" ='Y' THEN'Yes'
    ELSE 'Not indicated'
    END AS store_and_fwd_indication,
    "PULocationID" AS taximeter_engaged_zone,
    "DOLocationID" AS taximeter_disengaged_zone,
     CASE "payment_type"
        WHEN 0 THEN 'Flex Fare trip'
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
     ELSE 'Voided trip'
    END AS "payment_type", 
    "fare_amount",
    REPLACE("extra",'-','') as extra, 
    "mta_tax" AS metered_rate_tax,
    "tip_amount" AS creditcard_tip_amount,
    "tolls_amount",
    "improvement_surcharge",
    "total_amount" AS total_amount_excluding_cash_tip,
    "congestion_surcharge",
    "Airport_fee" AS LaGuardia_JFK_fee, 
    current_date AS last_updated_at
FROM raw_inc_yellow_tripdate_2024 
WHERE
    METADATA$ACTION = 'INSERT';

ALTER TASK load_new_nyc_data RESUME;

EXECUTE TASK load_new_nyc_data
DESCRIBE STREAM raw_stream_2024



select * from silver.transf_yellow_tripdate_2024


-----------------------------------------------------------------------------------------------------------------------------------------------------------
MERGE INTO silver.transf_yellow_tripdate_2024 AS target
USING (
        SELECT * 
        FROM raw_inc_yellow_tripdate_2024
        WHERE last_updated_at > (SELECT MAX(last_updated_at)
                                 FROM silver.transf_yellow_tripdate_2024)
 ) AS source
 ON target.vendor_id = source."VendorID"
        
WHEN MATCHED AND source.last_updated_at > target.last_updated_at THEN
UPDATE SET
vendor_id = source."VendorID"

WHEN NOT MATCHED THEN 
 INSERT INTO silver.transf_yellow_tripdate_2024
 (
 SELECT 
        ROW_NUMBER() OVER(ORDER BY "tpep_pickup_datetime") as primary_key,
        "VendorID" as vendor_id,
        TO_TIMESTAMP("tpep_pickup_datetime"::VARCHAR) as pickup_time,
        TO_TIMESTAMP("tpep_dropoff_datetime"::VARCHAR) as dropoff_time,
        date_trunc(month,TO_TIMESTAMP("tpep_pickup_datetime"::VARCHAR)) as months,
        "passenger_count" as total_passengers,
        "trip_distance",
       CASE
            WHEN "trip_distance"  BETWEEN  0.01 AND 79721.73  THEN  'Extra shorT' 
            WHEN "trip_distance"  BETWEEN  80721.74 AND 159443.46  THEN 'Short' 
            WHEN "trip_distance"  BETWEEN  159443.47 AND 239165.19 THEN 'Medium' 
            WHEN "trip_distance"  BETWEEN  239165.20 AND 318886.92 THEN 'Long' 
            WHEN "trip_distance"  BETWEEN  318886.93 AND 398608.62 THEN 'Exteme long' 
        ELSE 'None'
     END AS distance_group,
         CASE    "RatecodeID"
        WHEN  1 THEN 'Standard rate'
        WHEN  2 THEN 'JFK'
        WHEN  3 THEN 'Newark'
        WHEN  4 THEN 'Nassau or Westchester'
        WHEN  5 THEN 'Negotiated fare'
        WHEN  6 THEN 'Group ride'
        WHEN  99 THEN NulL
    ELSE null
    END AS ratecode_group,
     CASE 
        WHEN "store_and_fwd_flag" ='N' THEN 'No'
        WHEN "store_and_fwd_flag" ='Y' THEN'Yes'
    ELSE 'Not indicated'
    END AS store_and_fwd_indication,
    "PULocationID" AS taximeter_engaged_zone,
    "DOLocationID" AS taximeter_disengaged_zone,
     CASE "payment_type"
        WHEN 0 THEN 'Flex Fare trip'
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
     ELSE 'Voided trip'
    END AS "payment_type", 
    "fare_amount",
    REPLACE("extra",'-','') as extra, 
    "mta_tax" AS metered_rate_tax,
    "tip_amount" AS creditcard_tip_amount,
    "tolls_amount",
    "improvement_surcharge",
    "total_amount" AS total_amount_excluding_cash_tip,
    "congestion_surcharge",
    "Airport_fee" AS LaGuardia_JFK_fee, 
    current_date AS last_updated_at

FROM raw_inc_yellow_tripdate_2024 
WHERE "VendorID" IN (1,2,6,7) AND "payment_type" IN(0,1,2,3,4,5,6)
)

------------------------------------------------------------------------------------------------

   MERGE INTO silver.transf_yellow_tripdate_2024 AS target
    USING raw_stream_2024 AS source
    ON target."VendorID" = source.vendor_id
    
    WHEN MATCHED AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = TRUE THEN
        UPDATE SET target.vendor_id = source."VendorID"
        
    WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT'
    THEN
       
     INSERT INTO silver.transf_yellow_tripdate_2024
     (
     SELECT 
            ROW_NUMBER() OVER(ORDER BY "tpep_pickup_datetime") as primary_key,
            "VendorID" as vendor_id,
            TO_TIMESTAMP("tpep_pickup_datetime"::VARCHAR) as pickup_time,
            TO_TIMESTAMP("tpep_dropoff_datetime"::VARCHAR) as dropoff_time,
            date_trunc(month,TO_TIMESTAMP("tpep_pickup_datetime"::VARCHAR)) as months,
            "passenger_count" as total_passengers,
            "trip_distance",
           CASE
                WHEN "trip_distance"  BETWEEN  0.01 AND 79721.73  THEN  'Extra shorT' 
                WHEN "trip_distance"  BETWEEN  80721.74 AND 159443.46  THEN 'Short' 
                WHEN "trip_distance"  BETWEEN  159443.47 AND 239165.19 THEN 'Medium' 
                WHEN "trip_distance"  BETWEEN  239165.20 AND 318886.92 THEN 'Long' 
                WHEN "trip_distance"  BETWEEN  318886.93 AND 398608.62 THEN 'Exteme long' 
            ELSE 'None'
         END AS distance_group,
             CASE    "RatecodeID"
            WHEN  1 THEN 'Standard rate'
            WHEN  2 THEN 'JFK'
            WHEN  3 THEN 'Newark'
            WHEN  4 THEN 'Nassau or Westchester'
            WHEN  5 THEN 'Negotiated fare'
            WHEN  6 THEN 'Group ride'
            WHEN  99 THEN NulL
        ELSE null
        END AS ratecode_group,
         CASE 
            WHEN "store_and_fwd_flag" ='N' THEN 'No'
            WHEN "store_and_fwd_flag" ='Y' THEN'Yes'
        ELSE 'Not indicated'
        END AS store_and_fwd_indication,
        "PULocationID" AS taximeter_engaged_zone,
        "DOLocationID" AS taximeter_disengaged_zone,
         CASE "payment_type"
            WHEN 0 THEN 'Flex Fare trip'
            WHEN 1 THEN 'Credit card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No charge'
            WHEN 4 THEN 'Dispute'
            WHEN 5 THEN 'Unknown'
         ELSE 'Voided trip'
        END AS "payment_type", 
        "fare_amount",
        REPLACE("extra",'-','') as extra, 
        "mta_tax" AS metered_rate_tax,
        "tip_amount" AS creditcard_tip_amount,
        "tolls_amount",
        "improvement_surcharge",
        "total_amount" AS total_amount_excluding_cash_tip,
        "congestion_surcharge",
        "Airport_fee" AS LaGuardia_JFK_fee, 
        current_date AS last_updated_at
    
    FROM raw_inc_yellow_tripdate_2024 
    WHERE "VendorID" IN (1,2,6,7) AND "payment_type" IN(0,1,2,3,4,5,6)
    );

