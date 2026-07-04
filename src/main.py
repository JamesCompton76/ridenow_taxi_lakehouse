import os
import time
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, date_format, hour, unix_timestamp, md5,
    concat_ws, coalesce, lit, count, sum as _sum, round, input_file_name
)

VALID_PAYMENTS = [0, 1, 2, 3, 4, 5, 6]

def create_spark_session():
    # Clean, lightweight local session
    spark = SparkSession.builder \
        .appName("RideNow_Pipeline") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    # Silence the noisy INFO logs from the console
    spark.sparkContext.setLogLevel("WARN")

    return spark

def build_silver_layer(spark):
    print("\n--- 1. INGESTING RAW DATA ---")

    # Dynamically scan and log the files being ingested
    raw_dir = "/app/data/raw"
    parquet_files = glob.glob(f"{raw_dir}/*.parquet")
    print(f"📂 Found {len(parquet_files)} Parquet file(s) for ingestion:")
    for f in parquet_files:
        print(f"  - {os.path.basename(f)}")

    # Read all parquet files in the raw directory
    trip_data_path = f"{raw_dir}/*.parquet"
    zone_data_path = f"{raw_dir}/taxi_zone_lookup.csv"

    df_raw = spark.read.parquet(trip_data_path)
    # FIX: Explicitly infer schema to prevent string-vs-integer join mismatches
    df_zones = spark.read.option("header", "true").option("inferSchema", "true").csv(zone_data_path)

    print("\n--- 2. APPLYING SILVER TRANSFORMATIONS & QUARANTINE ---")
    df_derived = df_raw \
        .withColumn("pickup_date", to_date("tpep_pickup_datetime")) \
        .withColumn("pickup_month", date_format(col("tpep_pickup_datetime"), "yyyy-MM")) \
        .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
        .withColumn("trip_duration_mins", 
                    (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60)

    # Define strict data quality rules
    valid_condition = (
        (col("fare_amount") > 0) &
        (col("trip_distance").between(0, 100)) &
        (col("trip_duration_mins").between(0, 300)) &
        (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")) &
        (col("pickup_date") >= '2024-01-01') &
        (col("payment_type").isin(VALID_PAYMENTS))
    )

    # Evaluate each row against the rules
    df_evaluated = df_derived.withColumn("is_valid", valid_condition)

    # FAILS ON MY LIGHTWEIGHT DESIGN - COULD UPDATE SPARK SESSION TO USE MORE MEMORY IF AVAILABLE
    # OPTIMIZATION: Cache the lineage here to prevent re-reading raw files for subsequent separate actions
    #df_evaluated.cache()

    # Route bad data to Quarantine (handling True, False, and Null evaluations)
    df_quarantine = df_evaluated.filter((col("is_valid") == False) | col("is_valid").isNull()).drop("is_valid")
    df_silver = df_evaluated.filter(col("is_valid") == True).drop("is_valid")

    # Save the Quarantine data for auditing (Coalesce to 1 to avoid small file fragmentation)
    quarantine_count = df_quarantine.count()
    print(f"🚨 Routing {quarantine_count} invalid records to Quarantine for auditing...")
    df_quarantine.coalesce(1).write.mode("overwrite").parquet("/app/data/quarantine/rejected_trips")

    # Proceed with deduplication on the clean data
    df_silver = df_silver.withColumn(
        "surrogate_key", 
        md5(concat_ws("||", "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID", "VendorID", "total_amount"))
    ).dropDuplicates(["surrogate_key"])

    # TRADE-OFF: Left join to prevent silent data loss if zone IDs are missing
    df_silver = df_silver.join(
        df_zones,
        df_silver["PULocationID"] == df_zones["LocationID"],
        "left"
    ).withColumn(
        "valid_pu_borough", coalesce(col("Borough"), lit("Unknown"))
    ).drop("LocationID", "Borough", "Zone", "service_zone")

    print(f"✅ Silver transformations complete. Valid rows processed: {df_silver.count()}")
    return df_silver

def run_data_quality_tests(df_silver):
    print("\n--- 3. RUNNING DATA QUALITY TESTS (FAIL-FAST) ---")

    null_keys = df_silver.filter(col("surrogate_key").isNull()).count()
    assert null_keys == 0, f"DQ FAIL: Found {null_keys} null surrogate keys."

    out_of_bounds = df_silver.filter(~col("trip_duration_mins").between(0, 300)).count()
    assert out_of_bounds == 0, f"DQ FAIL: Found {out_of_bounds} trips outside 0-300 min range."

    invalid_payments = df_silver.filter(~col("payment_type").isin([0, 1, 2, 3, 4, 5, 6])).count()
    assert invalid_payments == 0, f"DQ FAIL: Found {invalid_payments} invalid payment types."

    print("✅ All Data Quality assertions passed on the Silver layer!")

def build_marts(df_silver):
    print("\n--- 5. COMPUTING & SAVING GOLD OUTPUTS ---")

    daily_metrics = df_silver.groupBy("pickup_date").agg(
        count("*").alias("total_trips"),
        round(_sum("total_amount"), 2).alias("total_revenue"),
        round(_sum("fare_amount") / coalesce(_sum("trip_distance"), lit(1)), 2).alias("avg_fare_per_mile")
    ).orderBy("pickup_date")

    tip_rate = df_silver.groupBy("valid_pu_borough", "pickup_hour").agg(
        round((_sum("tip_amount") / _sum("total_amount")) * 100, 2).alias("tip_rate_pct")
    ).orderBy("valid_pu_borough", "pickup_hour")

    # Output samples to console
    print("\nKPI: Daily Trips, Revenue, and Avg Fare/Mile (Showing Top 5)")
    daily_metrics.show(5)
    print("KPI: Tip Rate by Borough & Hour (Showing Top 5)")
    tip_rate.show(5)

    # Save Gold output (Coalesce to 1 to avoid small file fragmentation)
    print("💾 Saving Daily Metrics to local data/gold/daily_metrics...")
    daily_metrics.coalesce(1).write.mode("overwrite").parquet("/app/data/gold/daily_metrics")

    print("💾 Saving Tip Rates to local data/gold/tip_rates...")
    tip_rate.coalesce(1).write.mode("overwrite").parquet("/app/data/gold/tip_rates")

if __name__ == "__main__":
    start_time = time.time()
    spark = create_spark_session()

    df_silver = build_silver_layer(spark)
    run_data_quality_tests(df_silver)

    print("\n--- 4. SAVING SILVER LAYER ---")
    print("💾 Saving Cleaned Silver Data to local data/silver/taxi_trips (Partitioned by Month)...")

    # Repartitioning by month ensures exactly 1 optimally sized file per month folder
    df_silver.repartition("pickup_month").write.mode("overwrite").partitionBy("pickup_month").parquet("/app/data/silver/taxi_trips")

    build_marts(df_silver)

    # --- FINAL FILE COUNT VERIFICATION ---
    print("\n" + "="*50 + "\nFINAL PARQUET FILE STORAGE BREAKDOWN\n" + "="*50)
    paths_to_check = {
        "Quarantine": "/app/data/quarantine/rejected_trips",
        "Silver": "/app/data/silver/taxi_trips",
        "Gold Metrics": "/app/data/gold/daily_metrics",
        "Gold Tip Rates": "/app/data/gold/tip_rates"
    }

    for layer, path in paths_to_check.items():
        print(f"\n📁 Layer: {layer}")
        try:
            check_df = spark.read.parquet(path)
            (check_df.withColumn("file_path", input_file_name())
             .groupBy("file_path").agg(count("*").alias("row_count"))
             .orderBy("file_path").show(truncate=False))
        except Exception as e:
            print(f"  No files found or readable at {path}")
    print("="*50)

    spark.stop()
    end_time = time.time()
    print(f"\n🎉 PIPELINE COMPLETE.")
    print(f"⏱️ Total Execution Time: {end_time - start_time:.2f} seconds")
