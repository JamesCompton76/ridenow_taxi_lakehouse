SELECT 
    sum(CASE WHEN fare_amount <= 0 THEN 1 ELSE 0 END) as count_invalid_fare,
    sum(CASE WHEN trip_distance < 0 OR trip_distance > 100 THEN 1 ELSE 0 END) as count_invalid_distance,
    sum(CASE WHEN trip_duration_mins < 0 OR trip_duration_mins > 300 THEN 1 ELSE 0 END) as count_invalid_duration,
    sum(CASE WHEN tpep_dropoff_datetime <= tpep_pickup_datetime THEN 1 ELSE 0 END) as count_negative_trip_time,
    sum(CASE WHEN pickup_date < '2024-01-01' THEN 1 ELSE 0 END) as count_pre_2024,
    sum(CASE WHEN payment_type not in (0,1,2,3,4,5,6) THEN 1 ELSE 0 END) as  count_invalid_payment_type
FROM './data/quarantine/rejected_trips/*.snappy.parquet';
