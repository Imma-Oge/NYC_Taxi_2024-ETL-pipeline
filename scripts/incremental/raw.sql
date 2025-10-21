  select * from raw_stream_2024  
    


select * from raw_inc_yellow_tripdate_2024

copy into raw_inc_yellow_tripdate_2024
from @inc_load_dec_taxi/yellow_tripdata_2024-01.parquet
 FILE_FORMAT = parquett
 MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

 select * from raw_inc_yellow_tripdate_2024
