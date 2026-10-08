SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    PULocationID,
    DOLocationID,
    trip_distance,
    total_amount
FROM read_parquet('work/experiment/yellow_tripdata_2025-01.parquet')
ORDER BY total_amount, trip_distance, tpep_pickup_datetime;
