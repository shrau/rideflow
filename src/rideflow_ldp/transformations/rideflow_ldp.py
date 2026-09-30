from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = "/Volumes/rideflow/raw/landing_zone"


# ---------- TRIPS ----------

@dp.table(name="trips_bronze", comment="Raw trip events, exactly as they arrived")
def trips_bronze():
    return (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.inferColumnTypes", "true")
              .load(f"{LANDING}/trips/")
              .withColumn("_source_file", F.col("_metadata.file_path"))
              .withColumn("_ingested_at", F.current_timestamp()))


@dp.table(name="trips_silver", comment="Typed trips with data-quality expectations")
@dp.expect("valid_city", "city_id IN (1, 2, 3, 4, 5)")
@dp.expect_or_drop("completed_has_driver", "trip_status <> 'COMPLETED' OR driver_id IS NOT NULL")
@dp.expect_or_drop("fare_not_negative", "fare_inr >= 0")
@dp.expect_or_drop("dropoff_after_pickup", "pickup_ts IS NULL OR dropoff_ts >= pickup_ts")
@dp.expect_or_fail("trip_id_present", "trip_id IS NOT NULL")
def trips_silver():
    df = spark.readStream.table("trips_bronze")
    for c in ["request_ts", "pickup_ts", "dropoff_ts", "event_ts"]:
        df = df.withColumn(c, F.to_timestamp(c))
    return df.withColumn("trip_date_ist",
                         F.to_date(F.from_utc_timestamp("request_ts", "Asia/Kolkata")))


dp.create_streaming_table("trips_latest", comment="Latest version of each trip (SCD1)")

dp.create_auto_cdc_flow(
    target="trips_latest",
    source="trips_silver",
    keys=["trip_id"],
    sequence_by=F.col("event_ts"),
    stored_as_scd_type=1,
)


# ---------- DRIVERS ----------

@dp.table(name="drivers_cdc_bronze", comment="Raw driver CDC events")
def drivers_cdc_bronze():
    return (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.inferColumnTypes", "true")
              .load(f"{LANDING}/cdc/drivers/"))


dp.create_streaming_table("dim_driver", comment="Driver history (SCD2) built with AUTO CDC")

dp.create_auto_cdc_flow(
    target="dim_driver",
    source="drivers_cdc_bronze",
    keys=["driver_id"],
    sequence_by=F.col("sequence_num"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "sequence_num", "_rescued_data"],
    track_history_column_list=["driver_name", "phone", "city_id", "vehicle_type", "rating", "status"],
    stored_as_scd_type=2,
)



# ---------- GOLD ----------

@dp.materialized_view(name="daily_city_kpis", comment="One row per IST date per city")
def daily_city_kpis():
    trips  = spark.read.table("trips_latest").filter("city_id IN (1, 2, 3, 4, 5)")
    cities = spark.read.table("rideflow.silver.cities")          # reads a table from OUTSIDE the pipeline

    return (trips.join(cities, "city_id")
              .groupBy("trip_date_ist", "city_id", "city_name")
              .agg(
                  F.count("*").alias("total_requests"),
                  F.sum(F.when(F.col("trip_status") == "COMPLETED", 1).otherwise(0)).alias("completed_trips"),
                  F.round(100.0 * F.sum(F.when(F.col("trip_status") != "COMPLETED", 1).otherwise(0))
                          / F.count("*"), 1).alias("cancellation_rate_pct"),
                  F.round(F.sum(F.when(F.col("trip_status") == "COMPLETED", F.col("fare_inr"))), 2)
                          .alias("gross_revenue_inr"),
              ))