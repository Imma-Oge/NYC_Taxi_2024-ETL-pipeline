/*
dataset in the local repository was loaded to a staging area from whereit would be directly loaded to the bronze layer
This script shows how the database schemas  Bronze, silver and gold layers were created in snowflake using SQL.
Also SQL was used for the creation and as well the loading of parquet file into the stage area
*/
--to create  database and schema in snowflake
CREATE DATABASE full_load_NYC_TAXI;

CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;

USE SCHEMA bronze;



--create file  format
CREATE OR REPLACE FILE FORMAT parquet
    TYPE = PARQUET;

-- Create an internal stage.
CREATE OR REPLACE STAGE full_load_dec_taxi
FILE_FORMAT = parquet;

--to access files in stage
list '@full_load_dec_taxi'

--- Query the INFER_SCHEMA function.
 SELECT *
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION=>'@full_load_dec_taxi'
      , FILE_FORMAT=>'parquet'
      )
    );

---create table with the inferred schema 
    CREATE OR REPLACE TABLE yellow_tripdate_2024
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION => '@full_load_dec_taxi', 
      FILE_FORMAT => 'parquet'
    )
  )
      
  SELECT * FROM yellow_tripdate_2024


