from wbgps import *
from collections import Counter
from cpputils import get_stationary_events
from datetime import datetime, timezone, timedelta
from infomap import Infomap
from infostop.utils import query_neighbors
import numpy as np
import pandas as pd
import os
import pyspark.sql.functions as F
from pyspark.sql.functions import lag, col, countDistinct, to_timestamp, lit, from_unixtime,  pandas_udf, PandasUDFType
from pyspark.sql.window import Window
from pyspark.sql.types import *
from sklearn.cluster import DBSCAN


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
      .apply(get_stop_location)
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
