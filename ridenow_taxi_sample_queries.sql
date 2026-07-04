-- ==============================================================================
-- RideNow Analytics - Sample Queries (DuckDB)
-- To execute locally from repo root: duckdb < ridenow_taxi_sample_queries.sql
-- ==============================================================================

-- a. Daily trips, revenue, average fare per mile
SELECT 
    pickup_date,
    COUNT(*) AS total_trips,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(SUM(fare_amount) / NULLIF(SUM(trip_distance), 0), 2) AS avg_fare_per_mile
FROM read_parquet('data/silver/taxi_trips/**/*.parquet')
GROUP BY pickup_date
ORDER BY pickup_date;

-- ------------------------------------------------------------------------------

-- b. Top 10 origin-destination pairs per month
WITH ranked_routes AS (
    SELECT 
        pickup_month,
        PULocationID,
        DOLocationID,
        COUNT(*) as trip_count,
        ROW_NUMBER() OVER(PARTITION BY pickup_month ORDER BY COUNT(*) DESC) as rank
    FROM read_parquet('data/silver/taxi_trips/**/*.parquet')
    GROUP BY pickup_month, PULocationID, DOLocationID
)
SELECT 
    pickup_month,
    rank,
    PULocationID,
    DOLocationID,
    trip_count
FROM ranked_routes 
WHERE rank <= 10
ORDER BY pickup_month, rank;

-- ------------------------------------------------------------------------------

-- c. Tip-rate (%) by borough and pick up hour
SELECT 
    valid_pu_borough,
    pickup_hour,
    ROUND((SUM(tip_amount) / NULLIF(SUM(total_amount), 0)) * 100, 2) AS tip_rate_pct
FROM read_parquet('data/silver/taxi_trips/**/*.parquet')
GROUP BY valid_pu_borough, pickup_hour
ORDER BY valid_pu_borough, pickup_hour;

-- ------------------------------------------------------------------------------

-- d. Card vs cash share by month
-- Note: Assuming TLC standard where 1 = Credit Card and 2 = Cash
SELECT 
    pickup_month,
    COUNT(*) AS total_trips,
    ROUND(SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS card_share_pct,
    ROUND(SUM(CASE WHEN payment_type = 2 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS cash_share_pct
FROM read_parquet('data/silver/taxi_trips/**/*.parquet')
GROUP BY pickup_month
ORDER BY pickup_month;
