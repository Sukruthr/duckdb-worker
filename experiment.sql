SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    PULocationID,
    DOLocationID,
    trip_distance,
    total_amount
FROM trips
ORDER BY total_amount, trip_distance, tpep_pickup_datetime;
