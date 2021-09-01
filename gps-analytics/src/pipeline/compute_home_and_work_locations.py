from wbgps import *
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, TimestampType, DoubleType
import os
import argparse

parser = argparse.ArgumentParser(description='Pipeline arguments')
parser.add_argument('--country', type=str)
parser.add_argument('--stop_locations_dir', type=str)
parser.add_argument('--radius', type=int)
parser.add_argument('--stay_time', type=int)
parser.add_argument('--min_pts_per_stop_location', type=int)
parser.add_argument('--max_time_stop_location', type=int)
parser.add_argument('--max_accuracy', type=int)
parser.add_argument('--db_scan_radius', type=int)
parser.add_argument('--results_path_spark', type=str)
parser.add_argument('--start_hour_day', type=int)
parser.add_argument('--end_hour_day', type=int)
parser.add_argument('--min_pings_home_cluster_label', type=int)
parser.add_argument('--work_activity_average', type=float)
parser.add_argument('--hw', type=int)
parser.add_argument('--ww', type=int)
parser.add_argument('--wa', type=int)
parser.add_argument('--mph', type=float)
parser.add_argument('--mpw', type=float)

args = parser.parse_args()
country = args.country
stop_location_dir = args.stop_location_dir
radius = args.radius
stay_time = args.stay_time
min_pts_per_stop_location = args.min_pts_per_stop_location
max_time_stop_location = args.max_time_stop_location
max_accuracy = args.max_accuracy
db_scan_radius = args.db_scan_radius
results_path_spark=args.results_path_spark
start_hour_day=args.start_hour_day
end_hour_day=args.end_hour_day
min_pings_home_cluster_label=args.min_pings_home_cluster_label
work_activity_average=args.work_activity_average
hw=args.hw
ww=args.ww
mph=args.mph
mpw=args.mpw
suffix=f"hw{hw}_ww{ww}_wa{3600}_mph{mph}_mpw{mpw}"

#  we need to define this
#  start_hour_day, end_hour_day, min_pings_home_cluster_label, work_activity_average, country, end_date, suffix

results_path_spark = f"/mnt/Geospatial/results/veraset/{country}/accuracy{max_accuracy}_maxtimestop{max_time_stop_location}_staytime{stay_time}_radius{radius}_dbscanradius{db_scan_radius}/date{end_date}/"
df = spark.read.parquet(os.path.join(results_path_spark, 'stops_geocoded'))

df = df.withColumn("t_start_hour", F.hour(F.to_timestamp("t_start"))).withColumn("t_end_hour", F.hour(
    F.to_timestamp("t_end"))).withColumn(
    'weekday', F.dayofweek(F.to_timestamp("t_start"))).withColumn("date", F.to_timestamp("t_start")).withColumn(
    "date_trunc", F.date_trunc("day", F.col("date")))

df = df.withColumnRenamed(
    'latitude', 'lat').withColumnRenamed('longitude', 'lon')

schema_df = StructType([
    StructField('user_id', StringType(), False),
    StructField('t_start', LongType(), False),
    StructField('t_end', LongType(), False),
    StructField('duration', LongType(), False),
    StructField('lat', DoubleType(), False),
    StructField('lon', DoubleType(), False),
    StructField('total_duration_stop_location', LongType(), False),
    StructField('total_pings_stop', LongType(), False),
    StructField('cluster_label', LongType(), False),
    StructField('median_accuracy', DoubleType(), False),
    StructField('location_type', StringType(), True),
    StructField('home_label', LongType(), True),
    StructField('work_label', LongType(), True),
    StructField('geom_id', StringType(), False),
    StructField('date', TimestampType(), True),
    StructField('t_start_hour', IntegerType(), True),
    StructField('t_end_hour', IntegerType(), True),
    StructField("date_trunc", TimestampType(), True)
])
res_df = df.groupBy("user_id").apply(compute_home_work_label_dynamic, args=(
start_hour_day, end_hour_day, min_pings_home_cluster_label, work_activity_average))

fname = f"personal_stop_location_{suffix}"
res_df.write.mode("overwrite").parquet(
    os.path.join(results_path_spark, fname))

fname = f"personal_stop_location_{suffix}"
res_df = spark.read.parquet(os.path.join(results_path_spark, fname))

fname = f"durations_window_{suffix}"
res = get_durations(res_df, start_hour_day, end_hour_day)
res.write.mode("overwrite").parquet(os.path.join(results_path_spark, fname))
