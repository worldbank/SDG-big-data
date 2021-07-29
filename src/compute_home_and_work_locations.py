from pyspark import StorageLevel
import pyspark.sql.functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType, col, lit, lag, countDistinct, to_timestamp
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

results_path_spark = f"/mnt/Geospatial/results/veraset/{c.country}/accuracy100_maxtimestop3600_staytime300_radius50_dbscanradius50/date{c.end_date}/"
df = spark.read.parquet(os.path.join(c.results_path_spark, 'stops_geocoded'))

df = df.withColumn("t_start_hour", F.hour(F.to_timestamp("t_start"))).withColumn("t_end_hour", F.hour(F.to_timestamp("t_end"))).withColumn(
    'weekday', F.dayofweek(F.to_timestamp("t_start"))).withColumn("date", F.to_timestamp("t_start")).withColumn("date_trunc", F.date_trunc("day", F.col("date")))

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


def remove_unused_cols(final_df):
    return final_df.drop(columns=['weekday'])


def time_at_work(x, user_tmp):
    # 1. minimum fraction of 'duration' per day that you don't spend in your home_location for each cluster stop -> [10%,20%,30%]
    #   if user_tmp[user_tmp.location_type !='H'].empty:
    #     return np.NaN
    time_not_at_home = user_tmp[user_tmp.date_trunc ==
                                x.date_trunc]['duration'].sum()
    time_at_work = user_tmp[(user_tmp['cluster_label'] == x.cluster_label) & (
        user_tmp.date_trunc == x.date_trunc)]['duration'].sum()
    return time_at_work/time_not_at_home


def days_at_work_dynamic(x, user_tmp):
    # 2. minimum fraction of observed days of activity -> [20%,30%,40%]
    tmpdf = user_tmp[(user_tmp.date_trunc >= x.date_trunc-timedelta(
        days=c.work_period_window)) & (user_tmp.date_trunc <= x.date_trunc)]
    active_days = tmpdf['date_trunc'].nunique()

    tmpdf = tmpdf[(user_tmp.cluster_label == x.cluster_label)]
    work_days = tmpdf['date_trunc'].nunique()

    return work_days/active_days


# def days_at_home_dynamic(x, user_tmp):
#   # 2. minimum fraction of observed days of activity -> [20%,30%,40%]
#   tmpdf = user_tmp[(user_tmp.date_trunc>=x.date_trunc-timedelta(days=c.home_period_window))&(user_tmp.date_trunc<=x.date_trunc)]
#   active_days = tmpdf['date_trunc'].nunique()

#   tmpdf = tmpdf[(user_tmp.cluster_label==x.cluster_label)]
#   home_days = tmpdf['date_trunc'].nunique()

#   return home_days/active_days


def home_rolling_on_date(x):
    # the output dataframe will have as index the last date of the window and consider the previous "c.home_period_window" days to compute the window. Notice that this will be biased for the first c.home_period_window
    x = x.sort_values('date_trunc')
    x = x.set_index('date_trunc')
    tmp = x[['duration', 'total_pings_stop']].rolling(f'{c.home_period_window}D', min_periods=int(
        c.min_periods_over_window*c.home_period_window)).sum()
    tmp['days_count'] = x['duration'].rolling(
        f'{c.home_period_window}D').count()

    return tmp


def work_rolling_on_date(x):
    # if on average over "period" centered in "date" a candidate satisfy the conditions then for "date" is selected as WORK location
    x = x.sort_values('date_trunc')
    return x.set_index('date_trunc')[['duration']].rolling(f'{c.work_period_window}D', min_periods=int(c.min_periods_over_window_work*c.work_period_window)).mean()


@pandas_udf(schema_df, PandasUDFType.GROUPED_MAP)
def compute_home_work_label_dynamic(user_df):
    user_df['location_type'] = 'O'
    user_df['home_label'] = -1
    user_df['work_label'] = -1
#   raise Exception('Exception: columns',user_df.columns)
    # HOME
    # filter candidates as night-time stops
    home_tmp = user_df[(user_df['t_start_hour'] >= c.end_hour_day) | (
        user_df['t_end_hour'] <= c.start_hour_day)].copy()  # restrictive version of daytimes

    if home_tmp.empty:  # if we don't have at least a home location, we return and not attemp to compute work location
        return remove_unused_cols(user_df)
    first_day = home_tmp['date_trunc'].min()
    last_day = home_tmp['date_trunc'].max()
    # work on clusters with "duration" attribute per day ("date_trunc")
    home_tmp = home_tmp[['cluster_label', 'date_trunc', 'duration', 'total_pings_stop']].groupby(
        ['cluster_label', 'date_trunc']).sum().reset_index().sort_values('date_trunc')
    # computer cumulative duration of candidates over "period" window
    home_tmp = home_tmp.merge(home_tmp[['date_trunc', 'cluster_label', 'duration', 'total_pings_stop']].groupby(
        ['cluster_label']).apply(home_rolling_on_date).reset_index(), on=['date_trunc', 'cluster_label'], suffixes=('', '_cum'))
    print(home_tmp.columns)
######
    home_tmp = home_tmp[home_tmp.total_pings_stop_cum >
                        c.min_pings_home_cluster_label].drop('total_pings_stop_cum', axis=1)

    # filter out nan rows, equivalent to filter on min_days
    home_tmp = home_tmp.dropna(subset=['duration_cum'])

    if home_tmp.empty:  # if we don't have at least a home location, we return and not attemp to compute work location
        return remove_unused_cols(user_df)

    # Add minimum number of days per home location and filter based on it
    #home_tmp['n_days_home_cluster_label'] = home_tmp.apply(lambda x: days_at_home_dynamic(x, home_tmp), axis=1)
    #home_tmp = home_tmp[home_tmp['n_days_home_cluster_label'] >= c.min_days_per_home_location].drop('n_days_home_cluster_label', axis=1)

# comment below to allow for multiple home locations (diff from v4)
#   # label home_location on a day as the label with more duration over "period"
#   home_tmp['days_count_max'] = home_tmp.groupby('date_trunc')[['days_count']].transform(max)['days_count']
#   home_tmp = home_tmp[home_tmp['days_count_max'] == home_tmp['days_count']]

#   home_tmp['duration_cum_max'] = home_tmp.groupby('date_trunc')[['duration_cum']].transform(max)['duration_cum']
#   home_tmp = home_tmp[home_tmp['duration_cum_max'] == home_tmp['duration_cum']]

    #####################
    date_cluster = home_tmp.drop_duplicates(['cluster_label', 'date_trunc'])[
        ['date_trunc', 'cluster_label']].copy()
    date_cluster = date_cluster.drop_duplicates(['date_trunc'])
    home_label = list(zip(date_cluster.cluster_label, date_cluster.date_trunc))
    # creating a multinidex over which locating tuples of "date_trunc" and "home_label"
    idx = pd.MultiIndex.from_frame(user_df[['cluster_label', 'date_trunc']])
    user_df.loc[idx.isin(home_label), 'home_label'] = user_df.loc[idx.isin(
        home_label), 'cluster_label']
    #####################
    # interpolate home labels using the nearest available value
#   date_cluster = home_tmp.drop_duplicates(['cluster_label', 'date_trunc'])[['date_trunc', 'cluster_label']].copy()
#   date_cluster = date_cluster.drop_duplicates(['date_trunc'])
    base_dates = pd.date_range(start=first_day, end=last_day)
    date_cluster = date_cluster.sort_values(
        by='date_trunc').set_index('date_trunc')
    date_cluster = date_cluster.reindex(base_dates)
    if pd.notna(date_cluster['cluster_label']).sum() > 1:
        date_cluster = date_cluster.interpolate(
            method='nearest').ffill().bfill()
    else:
        date_cluster = date_cluster.ffill().bfill()
    date_cluster.index.name = 'date_trunc'
    date_cluster = date_cluster.reset_index()

    home_label = list(zip(date_cluster.cluster_label, date_cluster.date_trunc))
    # creating a multindex over which locating tuples of "date_trunc" and "home_label"
    idx = pd.MultiIndex.from_frame(user_df[['cluster_label', 'date_trunc']])

    user_df.loc[idx.isin(home_label), 'location_type'] = 'H'

    home_list = home_tmp.cluster_label.unique()
    if home_list.size == 0:
        return remove_unused_cols(user_df)

    ########
    # WORK #
    ########
    work_tmp = user_df[~(user_df['cluster_label'].isin(home_list))].copy()
    if work_tmp.empty:  # if we can't compute work location we return
        return remove_unused_cols(user_df)
#   if daytime: ######don't like it
    work_tmp = work_tmp[((work_tmp['t_start_hour'] >= c.start_hour_day+4) & (work_tmp['t_end_hour']
                                                                             <= c.end_hour_day-6)) & (~work_tmp['weekday'].isin([1, 7]))]  # restrictive version of daytimes

    if work_tmp.empty:  # if we can't compute work location we return
        return remove_unused_cols(user_df)
    first_day = work_tmp['date_trunc'].min()
    # drop duplicates, smooth over "period" time window
    work_tmp = work_tmp[['date_trunc', 'cluster_label', 'duration']].groupby(
        ['cluster_label', 'date_trunc']).sum().reset_index()

    work_tmp = work_tmp.merge(work_tmp[['date_trunc', 'cluster_label', 'duration']]
                              .groupby(['cluster_label'])
                              .apply(work_rolling_on_date)
                              .reset_index(), on=['date_trunc', 'cluster_label'], suffixes=('', '_average'))

    # filter out candidates which on average on the period do not pass the constraint
    work_tmp = work_tmp[(work_tmp.duration_average >= c.work_activity_average)]
    # Select work clusters candidate: the clusters that passed the previous criteria are selected as work for the day
    if work_tmp.empty:  # if we can't compute work location we return
        return remove_unused_cols(user_df)

    #####################
    work_label = list(zip(work_tmp.cluster_label, work_tmp.date_trunc))
    idx = pd.MultiIndex.from_frame(user_df[['cluster_label', 'date_trunc']])
    work_list = work_tmp.cluster_label.unique()
    if work_list.size == 0:
        return remove_unused_cols(user_df)
    # add cluster label to work_label on the day on which it is found to be work_location only
    user_df.loc[idx.isin(work_label), 'work_label'] = user_df.loc[idx.isin(
        work_label), 'cluster_label']
    #####################
    # add work labels to all user dataset
    work_label = work_tmp['cluster_label'].unique()
    idx = pd.Index(user_df['cluster_label'])
    user_df.loc[idx.isin(work_label), 'location_type'] = 'W'

    return remove_unused_cols(user_df)


res_df = df.groupBy("user_id").apply(compute_home_work_label_dynamic)
fname = f"personal_stop_location_{c.suffix}"
res_df.write.mode("overwrite").parquet(
    os.path.join(c.results_path_spark, fname))

fname = f"personal_stop_location_{c.suffix}"
res_df = spark.read.parquet(os.path.join(c.results_path_spark, fname))


def get_durations(durations):
    durations = (durations
                 .withColumn('hour', F.hour('date'))
                 .withColumn('day_night', F.when((col('hour') >= c.start_hour_day) & (col('hour') < c.end_hour_day), 'day')
                                           .when((col('hour') < c.start_hour_day) | (col('hour') >= c.end_hour_day), 'night')
                                           .otherwise(None))  # stops that cross day/night boundaries
                 .select('user_id', 'date_trunc', 'day_night', 'location_type', 'duration'))
    total_durations = durations.groupby('date_trunc', 'user_id').agg(
        F.sum('duration').alias('total_duration'))
    durations = durations.groupby('date_trunc', 'day_night', 'user_id').pivot(
        'location_type').agg(F.sum('duration').alias('duration'))
    durations = durations.join(total_durations, on=['date_trunc', 'user_id'])
    durations = durations.withColumn('absolute_H',  col('H'))
    durations = durations.withColumn('absolute_W',  col('W'))
    durations = durations.withColumn('absolute_O', col('O'))
    durations = durations.withColumn('H', col('H') / col('total_duration'))
    durations = durations.withColumn('W', col('W') / col('total_duration'))
    durations = durations.withColumn(
        'O', col('O') / col('total_duration')).drop('total_duration')
    return durations


fname = f"durations_window_{c.suffix}"
res = get_durations(res_df)
res.write.mode("overwrite").parquet(os.path.join(c.results_path_spark, fname))
