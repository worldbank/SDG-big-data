from collections import Counter
from cpputils import get_stationary_events
from datetime import timezone
import numpy as np
from pyspark.sql.functions import lag, col, countDistinct, to_timestamp, lit, from_unixtime, pandas_udf, PandasUDFType
from sklearn.cluster import DBSCAN

from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, TimestampType, DoubleType


def get_most_frequent_label(a):
    if a.size > 0:
        cnt = Counter(a)
        return cnt.most_common(1)[0][0]
    return None


def compute_intervals(centroids, labels, timestamps, accuracy, input_data):
    # if the label is -1 it means that the point doesn't belong to any cluster. Otherwise there should be at least 2 points for a stop locations
    # and they should
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
            trajectory.append((timestamps[start_index], timestamps[i], *centroids[seen],
                               np.median(accuracy[start_index: i]), i - start_index + 1))
            seen += 1
            i += 1

    return trajectory


def data_assertions(data):
    assert np.all(data[:-1, 2] <= data[1:, 2]), "Timestamps must be ordered"
    assert (np.min(data[:, 0]) > -90 and np.max(data[:, 0]) <
            90), "lat (column 0) must have values between -90 and 90"
    assert (np.min(data[:, 1]) > -180 and np.max(data[:, 1]) <
            180), "lon (column 1) must have values between -180 and 180"


def run_infostop(data, r1, min_staying_time, min_size, max_time_between, distance_metric):
    data_assertions(data)
    centroids, stat_labels = get_stationary_events(
        data[:, :3], r1, min_size, min_staying_time, max_time_between, distance_metric)
    return compute_intervals(centroids, stat_labels, data[:, 2], data[:, 3], data)


def to_unix_int(date):
    return int(date.replace(tzinfo=timezone.utc).timestamp())


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


@pandas_udf(schema_df, PandasUDFType.GROUPED_MAP)
def get_stop_location(df, radius, stay_time, min_pts_per_stop_location, max_time_stop_location, max_accuracy,
                      db_scan_radius):
    identifier = df['user_id'].values[0]
    df.sort_values(by='epoch_time', inplace=True)  # shouldn't be necessary

    data = df[["lat", "lon", 'epoch_time', "accuracy"]].values
    res = run_infostop(data, r1=radius, min_staying_time=stay_time, min_size=min_pts_per_stop_location,
                       max_time_between=max_time_stop_location, distance_metric='haversine')

    df = pd.DataFrame(res, columns=[
        "t_start", "t_end", "lat", "lon", "median_accuracy", "total_pings_stop"])

    # new filtering step based on median accuracy
    df = df[df['median_accuracy'] < max_accuracy]

    df['user_id'] = identifier
    if not df.empty:
        #       df['cluster_label'] = get_labels(df[['lat', 'lon']])
        # notice that we don't have noise here, since any point that we consider is a stop location and hence has been already pre filtered by run_infostop (min_samples = 1 => no label =-1)
        db = DBSCAN(eps=db_scan_radius, min_samples=1, metric='haversine',
                    algorithm='ball_tree').fit(np.radians(df[['lat', 'lon']].values))
        df['cluster_label'] = db.labels_
    else:
        df['cluster_label'] = None
    return df


schema_cluster_df = StructType([
    StructField('user_id', StringType(), False),
    StructField('lat', DoubleType(), False),
    StructField('lon', DoubleType(), False),
    StructField('cluster_label', LongType(), True),
    StructField('median_accuracy', DoubleType(), True),
    StructField('total_pings_stop', LongType(), True),
    StructField('total_duration_stop_location', LongType(), True),
    StructField('t_start', LongType(), False),
    StructField('t_end', LongType(), False),
    StructField('duration', LongType(), False),
])


@pandas_udf(schema_cluster_df, PandasUDFType.GROUPED_MAP)
def get_stop_cluster(df, db_scan_radius):
    if not df.empty:
        # notice that we don't have noise here, since any point that we consider is a stop location and hence has been already pre filtered by run_infostop (min_samples = 1 => no label =-1)
        db = DBSCAN(eps=db_scan_radius, min_samples=1, metric='haversine',
                    algorithm='ball_tree').fit(np.radians(df[['lat', 'lon']].values))
        df['cluster_label'] = db.labels_
    else:
        df['cluster_label'] = None
    return df
