SELECT
    fare_amount as count_invalid_fare, COUNT(*) as qty
FROM './data/quarantine/rejected_trips/*.snappy.parquet'
WHERE fare_amount <= 0
GROUP BY fare_amount
ORDER BY fare_amount ASC;
