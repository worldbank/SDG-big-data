from collections import Counter
from cpputils import get_stationary_events
from datetime import datetime, timezone, timedelta
from infomap import Infomap
from infostop.utils import query_neighbors
import time
import numpy as np
import pandas as pd
import os
import pyspark.sql.functions as F
from pyspark.sql.functions import lag, col, countDistinct, to_timestamp, lit, from_unixtime,  pandas_udf, PandasUDFType
from pyspark.sql.window import Window
from pyspark.sql.types import *
from sklearn.cluster import DBSCAN

dates_computed = os.listdir('/dbfs'+c.stop_locations_dir[:-15])
dates_f = [datetime.strptime(date[4:], '%Y-%m-%d') for date in dates_computed]
old_end_date = dates_computed[dates_f.index(max(dates_f))]

def get_most_frequent_label(a):
  if a.size>0:
    cnt = Counter( a)
    return cnt.most_common(1)[0][0]
  return None

def compute_intervals(centroids, labels, timestamps, accuracy, input_data):
     # if the label is -1 it means that the point doesn't belong to any cluster. Otherwise there should be at least 2 points for a stop locations
    #and they should 
#     assert (len(centroids) == len(community_labels))
    i = 0
    seen = 0 
    trajectory = []
    while i < len(labels):
        if labels[i] == -1:
            i += 1
        else:
            start_index = i
            while (i + 1 < len(labels)) and (labels[i] == labels[i + 1]):
                i += 1
            trajectory.append(( timestamps[start_index], timestamps[i], *centroids[seen], np.median(accuracy[start_index : i]), i - start_index + 1))
            seen += 1
            i += 1

    return trajectory
  
def data_assertions(data):
  assert np.all(data[:-1, 2] <= data[1:, 2]), "Timestamps must be ordered" 
  assert (np.min(data[:, 0]) > -90 and np.max(data[:, 0]) < 90),         "lat (column 0) must have values between -90 and 90" 
  assert (np.min(data[:, 1]) > -180 and np.max(data[:, 1]) < 180),    "lon (column 1) must have values between -180 and 180"

  

def run_infostop(data, r1, min_staying_time, min_size, max_time_between, distance_metric):
    data_assertions(data)
    centroids, stat_labels = get_stationary_events(data[:,:3], r1, min_size, min_staying_time, max_time_between, distance_metric)
    return compute_intervals(centroids, stat_labels, data[:, 2], data[:,3], data)
  
  
schema_df =StructType([
  StructField('user_id'     , StringType()   , False),
  StructField('t_start'   , LongType()   , False),
  StructField('t_end', LongType() , False),
  StructField('lat'     , DoubleType()   , False),
  StructField('lon'  , DoubleType()    , False),
  StructField('cluster_label', LongType()    , True),
  StructField('median_accuracy'  , DoubleType()    , True),
  StructField('total_pings_stop', LongType()   , True),
])

# https://github.com/ulfaslak/infostop/issues/9
@pandas_udf(schema_df, PandasUDFType.GROUPED_MAP)
def get_stop_location(df):
    
    identifier =  df['user_id'].values[0]
    df.sort_values(by='epoch_time', inplace=True) #shouldnt be necessary

    data = df[["lat", "lon", 'epoch_time', "accuracy"]].values
    res = run_infostop(data, r1=c.radius, min_staying_time=c.stay_time, min_size=c.min_pts_per_stop_location, max_time_between=c.max_time_stop_location, distance_metric='haversine')
  
    df = pd.DataFrame(res, columns=["t_start",  "t_end", "lat", "lon", "median_accuracy", "total_pings_stop"])
    
    # new filtering step based on median accuracy
    df = df[df['median_accuracy'] < c.max_accuracy]
    
    df['user_id'] = identifier
    df['cluster_label'] = -1
    return df

schema_cluster_df = StructType([
  StructField('user_id'     , StringType()   , False),
  StructField('lat'     , DoubleType()   , False),
  StructField('lon'  , DoubleType()    , False),
  StructField('cluster_label', LongType()    , True),
  StructField('median_accuracy'  , DoubleType()    , True),
  StructField('total_pings_stop', LongType()   , True),
  StructField('total_duration_stop_location', LongType()   , True),
  StructField('t_start'   , LongType()   , False),
  StructField('t_end', LongType() , False),
  StructField('duration', LongType() , False),
])
@pandas_udf(schema_cluster_df, PandasUDFType.GROUPED_MAP)
def get_stop_cluster(df):
    if not df.empty:
          db = DBSCAN(eps=c.db_scan_radius, min_samples=1, metric='haversine', algorithm='ball_tree').fit(np.radians(df[['lat', 'lon']].values)) # notice that we don't have noise here, since any point that we consider is a stop location and hence has been already pre filtered by run_infostop (min_samples = 1 => no label =-1)
          df['cluster_label'] = db.labels_
    else:
      df['cluster_label'] = None
    return df

def _to_unix_int(date):
  return int(date.replace(tzinfo=timezone.utc).timestamp())

# from a start and end date that span multiple days, return one start and end date per day
@udf(ArrayType(StructType([StructField("t_start", LongType()), StructField("t_end", LongType())])))
def make_list(start, end):
    parts = list(pd.date_range(start, end, freq='d', normalize=True))
    #if all the dates fall in the same day, return them without modifications
    if not parts:
        return [(_to_unix_int(start), _to_unix_int(end))]
    res = []
    # the dates are normalized to midnight, so we need to append the end date if it is not midnight
    if parts[-1] != end:
        parts.append(end)
    #set the initial date instead of the normalized one from the daterange
    parts[0] = start
    for i in range(len(parts) - 1):
        if i != len(parts) - 1:
          # dates are normalized to midnight
            res.append((_to_unix_int(parts[i]), _to_unix_int(parts[i+1])))
        else:
            res.append(_to_unix_int(parts[i]), _to_unix_int(parts[i+1]))
    return res

old_file_date = os.path.join(c.stop_locations_dir, '..', old_end_date)
current = spark.read.parquet(old_file_date)
last_date = int(datetime.timestamp(datetime.strptime(old_end_date[4:], "%Y-%m-%d"))) - (60*60*24*2) ## Get the last date on file and go back one days

current = current.where(F.col("t_start") < last_date)

end_date = int(datetime.timestamp(datetime.strptime(c.end_date, "%Y-%m-%d")))

# filter_string = f"accuracy >=0 AND accuracy <= 200 AND lat > -90 AND lat < 90 AND lon > -180 AND lon < 180"
filter_string = f"accuracy >=0 AND lat > -90 AND lat < 90 AND lon > -180 AND lon < 180 AND timestamp >= {last_date} AND timestamp < {end_date}"

if not tz: # add a check on TZ_OFFSET
  pings = spark.sql(f"SELECT  device_id AS user_id, lat, lon, accuracy, timestamp, TZ_OFFSET_SEC FROM default.veraset_{c.country}_tz WHERE country = '{c.country}' AND {filter_string}")
  pings = (pings
           .withColumn('epoch_time', col("timestamp") + col("TZ_OFFSET_SEC").cast("long"))
           .drop("TZ_OFFSET_SEC", "timestamp"))
elif tz:
  pings = spark.sql(f"SELECT  device_id AS user_id, lat, lon, accuracy, timestamp FROM default.veraset_primary_1  WHERE country = '{c.country}' AND {filter_string}")
  pings = (pings
           .withColumn('time', F.to_timestamp('timestamp'))
           .withColumn('new_time', F.from_utc_timestamp('time', tz))
           .withColumn('epoch_time', F.unix_timestamp('new_time'))
           .drop('timestamp', 'time', 'new_time'))
else:
  raise Exception ("Undefined time zone in config or tz_offset in input table")
  
sl = (pings
      .orderBy("epoch_time")
      .groupBy("user_id")
      .apply(get_stop_location)
      .dropna())

#split stop location that span mutiple days into single days
sl = (sl
      .withColumn("total_duration_stop_location", F.col("t_end") - F.col("t_start"))
      .withColumn('my_list', make_list(F.to_timestamp(col('t_start')), F.to_timestamp(col('t_end'))))
      .drop('t_start', 't_end')
      .withColumn("tmp", F.explode("my_list"))
      .withColumn("t_start", F.col("tmp").t_start)
      .withColumn("t_end", F.col("tmp").t_end)
      .drop("tmp", "my_list")
      .withColumn("duration", F.col("t_end") - F.col("t_start")))

sl_cluster = (current
              .union(sl)
              #.dropDuplicates(["user_id", "t_start"])
              .groupBy("user_id")
              .apply(get_stop_cluster))

sl_cluster.write.mode("overwrite").parquet(c.stop_locations_dir)