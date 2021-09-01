from wbgps import *
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import *
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
args = parser.parse_args()
country = args.country
stop_location_dir = args.stop_location_dir
radius = args.radius
stay_time = args.stay_time
min_pts_per_stop_location = args.min_pts_per_stop_location
max_time_stop_location = args.max_time_stop_location
max_accuracy = args.max_accuracy
db_scan_radius = args.db_scan_radius


schema_df = StructType([
    StructField('user_id', StringType(), False),
    StructField('t_start', LongType(), False),
    StructField('t_end', LongType(), False),
    StructField('lat', DoubleType(), False),
    StructField('lon', DoubleType(), False),
    StructField('cluster_label', LongType(), True),
    StructField('median_accuracy', DoubleType(), True),
    StructField('total_pings_stop', LongType(), True),
])

@udf(ArrayType(StructType([StructField("t_start", LongType()), StructField("t_end", LongType())])))
def make_list(start, end):
    parts = list(pd.date_range(start, end, freq='d', normalize=True))
    # if all the dates fall in the same day, return them without modifications
    if not parts:
        return [(to_unix_int(start), to_unix_int(end))]
    res = []
    # the dates are normalized to midnight, so we need to append the end date if it is not midnight
    if parts[-1] != end:
        parts.append(end)
    # set the initial date instead of the normalized one from the daterange
    parts[0] = start
    for i in range(len(parts) - 1):
        if i != len(parts) - 1:
            # dates are normalized to midnight
            res.append((to_unix_int(parts[i]), to_unix_int(parts[i+1])))
        else:
            res.append(to_unix_int(parts[i]), to_unix_int(parts[i+1]))
    return res


filter_string = f"accuracy >=0 AND accuracy <= 200 AND lat > -90 AND lat < 90 AND lon > -180 AND lon < 180"

if not tz:  # add a check on TZ_OFFSET
    pings = spark.sql(
        f"SELECT  device_id AS user_id, lat, lon, accuracy, timestamp, TZ_OFFSET_SEC FROM default.veraset_{c.country}_tz WHERE country = '{c.country}' AND {filter_string}")
    pings = (pings
             .withColumn('epoch_time', col("timestamp") + col("TZ_OFFSET_SEC").cast("long"))
             .drop("TZ_OFFSET_SEC", "timestamp"))
elif tz:
    pings = spark.sql(
        f"SELECT  device_id AS user_id, lat, lon, accuracy, timestamp FROM default.veraset_primary_1  WHERE country = '{c.country}' AND {filter_string}")
    pings = (pings
             .withColumn('time', F.to_timestamp('timestamp'))
             .withColumn('new_time', F.from_utc_timestamp('time', tz))
             .withColumn('epoch_time', F.unix_timestamp('new_time'))
             .drop('timestamp', 'time', 'new_time'))
else:
    raise Exception(
        "Undefined time zone in config or tz_offset in input table")

sl = (pings
      .orderBy("epoch_time")
      .groupBy("user_id")
      .apply(get_stop_location, args=(radius, stay_time, min_pts_per_stop_location, max_time_stop_location, max_accuracy, db_scan_radius))
      .dropna())

# split stop location that span mutiple days into single days
sl = (sl
      .withColumn("total_duration_stop_location", F.col("t_end") - F.col("t_start"))
      .withColumn('my_list', make_list(F.to_timestamp(col('t_start')), F.to_timestamp(col('t_end'))))
      .drop('t_start', 't_end')
      .withColumn("tmp", F.explode("my_list"))
      .withColumn("t_start", F.col("tmp").t_start)
      .withColumn("t_end", F.col("tmp").t_end)
      .drop("tmp", "my_list")
      .withColumn("duration", F.col("t_end") - F.col("t_start")))


sl.write.mode("overwrite").parquet(c.stop_locations_dir)
