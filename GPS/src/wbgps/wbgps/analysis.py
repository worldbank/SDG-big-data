### WBGPS analysis functions
dbutils.library.installPyPI("pandas", "1.3.1")
import os
import string
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import datetime as dt

import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, desc, lit
from pyspark.sql import Window


def google_change_metric(df_original, start_baseline, end_baseline,other_groups=[]):
  '''
  INPUT:  dataframe with (at least) 2 columns named "mean" and "sem"
  OUTPUT: dataframe with the values of the two columns converted to google
          change metric
  NOTE: Google uses as baseline period the 5-weeks period from Jan 3 to Feb 6
  '''
  df = df_original.copy()
  
  # compute weekday baseline values
  baseline = df.loc[start_baseline:end_baseline,['mean','sem']+other_groups].copy()
  baseline['weekday'] = list(baseline.index.dayofweek.values)
  baseline = baseline.groupby(['weekday']+other_groups,dropna=False,as_index=False).mean()
  df['weekday'] = list(df.index.dayofweek.values)

  date = df.index.copy()
  df = df.merge(baseline, on=['weekday']+other_groups, how='left',
                suffixes=('', '_baseline'))
  
  # compute "mean" change with respect to weekday baseline values
  df['mean'] = (df['mean']- df['mean_baseline']) / np.abs(df['mean_baseline'])
  df['sem'] = np.abs(df['sem']/df['mean_baseline'])
  df.index = date
  # return input dataframe with "mean" and "sem" column now expressing the relative change and its error
  return df.drop(['weekday','mean_baseline'],axis=1,errors='ignore')


def base_diff_metric(df_original, frac, start_baseline, end_baseline,other_groups=[]):
  '''
  INPUT:  dataframe with (at least) 2 columns named "mean" and "sem"
  OUTPUT: dataframe with the values of the two columns converted to change wrt the baseline
  '''
  df = df_original.rename(columns={frac:'mean'}).reset_index().set_index('date').copy()
  baseline = df.loc[start_baseline:end_baseline,['mean']+other_groups].copy()
  baseline['weekday'] = list(baseline.index.dayofweek.values)
  baseline = baseline.groupby(['weekday']+other_groups,dropna=False,as_index=False).mean()
  df['weekday'] = list(df.index.dayofweek.values)

  date = df.index.copy()
  df = df.merge(baseline, on=['weekday']+other_groups, how='left',suffixes=('', '_baseline'))
  df['mean'] = (df['mean']- df['mean_baseline'])
  df.index = date
  res2 = df.drop(['weekday','mean_baseline'],axis=1,errors='ignore')
  return res2


def process_admin(country,admin_path):
  '''
  INPUT:  country ISO code, path to the admin files (saved as "{country}/admin.csv" files)
  OUTPUT: three pandas dataframes
          - "admins_by_country": all country administrative units with socio-economic group assigned based on entire country 
          - "admins_by_metro_area": 
  '''
  cols = ['geom_id', 'metro_area_name', 'pop', 'wealth_index']
  admin = spark.read.option('header', 'true').csv(admin_path+f'{country}/admin.csv').toPandas()
  admins = admins[[cols]]

  admins = admins.rename(columns={'metro_ar_1': 'metro_area_name', 'wealth_ind': 'wealth_index'})

  admins_by_country = admins[['geom_id', 'pop', 'wealth_index']].dropna(
  ).sort_values(by=['wealth_index'], ascending=[False]).reset_index(drop=True)
  admins_by_country['pct_wealth'] = admins_by_country['pop'].cumsum().divide(
      admins_by_country['pop'].sum())

  admins_by_metro_area = admins[['geom_id', 'metro_area_name', 'pop', 'wealth_index']].dropna(
  ).sort_values(by=['metro_area_name', 'wealth_index'], ascending=[True, False]).reset_index(drop=True)
  admins_by_metro_area['pct_wealth'] = admins_by_metro_area.groupby(
      'metro_area_name')['pop'].apply(lambda x: x.cumsum()/x.sum())

  pop_metro_areas = admins_by_metro_area.groupby(
      'metro_area_name')['pop'].sum().sort_values(ascending=False)

  return admins_by_country, admins_by_metro_area, pop_metro_areas


def get_active_list(durations, country, activity_level):
  '''
  For each country invoke the following function to get a list of all active individuals.
    INPUT: dataframe with precomputed individual stops' durations, country ISO code, minimum activity level required
    OUTPUT: list of active individuals "user_id"
  '''
    # Indonesia experiences a major dropout from the service during January 2020. For this reason, a specific pre-pandemic period was adopted
  if country == 'ID':
      durations_2 = durations.where(col('date_trunc') >= '2020-02-01')
  else:
      durations_2 = durations
  durations_2 = durations_2.where(col('date_trunc') < '2021-01-01')

  active_days = (durations_2
                 .withColumn('pandemic', F.when(col('date_trunc') < '2020-03-15', 'pre').otherwise('post'))
                 .groupby('user_id', 'pandemic')
                 .agg(F.countDistinct('date_trunc').alias('n_days')))
  active_days.cache()

  max_days_pre = (active_days
                  .where(col('pandemic') == 'pre')
                  .agg(F.max('n_days').alias('max_days_pre'))
                  .toPandas().loc[0, 'max_days_pre'])

  max_days_all = (active_days
                  .groupby('user_id')
                  .agg(F.sum('n_days').alias('n_days'))
                  .agg(F.max('n_days').alias('max_days_all'))
                  .toPandas().loc[0, 'max_days_all'])

  active_users = (active_days
                  .groupby('user_id')
                  .pivot('pandemic')
                  .agg(F.first('n_days'))
                  .fillna(0)
                  .withColumn('tot', col('pre')+col('post'))
                  .where(col('pre') >= activity_level*max_days_pre)
                  .where(col('tot') >= activity_level*max_days_all))

  active_days.unpersist()

  return active_users


def compute_durations_and_admins(country, data_date, stop_path, activity_level=0,
                                 hw=28, ww=28, wa=900, mph=10, mpw=7):
  personal_nf = f"personal_stop_location_hw{hw}_ww{ww}_wa{wa}_mph{mph}_mpw{mpw}"
  stops = spark.read.parquet(f"{stop_path}{country}/accuracy100_maxtimestop3600_staytime300_radius50_dbscanradius50/date{data_date}/" + personal_nf)

  fname_nf = f'durations_window_hw{hw}_ww{ww}_wa{wa}_mph{mph}_mpw{mpw}'
  durations_path_nf = f'{stop_path}{country}/accuracy100_maxtimestop3600_staytime300_radius50_dbscanradius50/date{data_date}/' + fname_nf
  durations = spark.read.parquet(durations_path_nf)

  # aggregate day/night
  durations = (durations
               .groupby('date_trunc', 'user_id')
               .agg(F.sum('H').alias('H'),
                    F.sum('W').alias('W'),
                    F.sum('O').alias('O')))

  active_users = get_active_list(durations, country, activity_level)

  durations = durations.join(active_users.select(
      'user_id'), on='user_id', how='inner')

  # create binary column for commuters
  durations = durations.withColumn(
      'C', F.when(col('W').isNull(), 0).otherwise(1))

  # create binary column for people who don't leave home, aka recluse
  durations = durations.withColumn('R', F.when(
      (col('W').isNull()) & (col('O').isNull()), 1).otherwise(0))

  # compute H and W id for wealth labels
  w = Window.partitionBy('user_id')
  user_h_id = (stops
               .where(col('location_type') == 'H')
               .where(col('date_trunc') <= '2020-03-15')
               .groupby('user_id', 'geom_id')
               .agg(F.countDistinct('date_trunc').alias('n_days'))
               .withColumn('max_days', F.max('n_days').over(w))
               .where(col('n_days') == col('max_days'))
               .groupby('user_id')
               .agg(F.first('geom_id').alias('geom_id_home')))
  user_w_id = (stops
               .where(col('location_type') == 'W')
               # .where(col('date_trunc') <= '2020-03-15')
               .groupby('user_id', 'geom_id')
               .agg(F.countDistinct('date_trunc').alias('n_days'))
               .withColumn('max_days', F.max('n_days').over(w))
               .where(col('n_days') == col('max_days'))
               .groupby('user_id')
               .agg(F.first('geom_id').alias('geom_id_work')))

  durations_and_admins = (durations
                          .withColumnRenamed('date_trunc', 'date')
                          .select('date', 'user_id', 'H', 'R', 'W', 'C', 'O')
                          .join(user_h_id, on='user_id', how='left')
                          .join(user_w_id, on='user_id', how='left'))

  return durations_and_admins


def compute_durations_normalized_by_wealth_home(durations_and_admins, admins, labels_wealth, bins_wealth):
  admins['wealth_label'] = pd.cut(
      admins['pct_wealth'], bins_wealth, labels=labels_wealth)
  admins['geom_id'] = admins['geom_id'].astype(str)
  admins['wealth_label'] = admins['wealth_label'].astype(str)
  # get admin info for home and work location
  tmp1 = spark.createDataFrame(
      admins[['geom_id', 'pop', 'pct_wealth', 'wealth_label']].rename(columns=lambda x: x+'_home'))
  out1 = (durations_and_admins
          .join(tmp1, on='geom_id_home', how='inner'))

  geom_users = (out1
                .groupby('geom_id_home')
                .agg(F.countDistinct('user_id').alias('n_users')))

  out = (out1
         .join(geom_users, on='geom_id_home', how='inner')
         .withColumn('weight', col('pop_home')/col('n_users')))
  return out

def output(out, column):
  # compute aggregate measures
  out = (out
         .fillna(0, subset=column)
         .groupby('date', 'wealth_label_home')
         .agg((F.sum(col(column)*col('weight'))/F.sum(col('weight'))).alias('mean'),
              F.stddev(column).alias('std'),
              F.count(column).alias('n'),
              F.countDistinct('user_id').alias('n_unique'))
         .withColumn('sem', col('std')/F.sqrt(col('n')))
         .drop('std'))

  durations_normalized_by_wealth_home = out.toPandas(
  ).set_index(['wealth_label_home', 'date'])
  return durations_normalized_by_wealth_home


def compute_durations_normalized_by_wealth_home_wealth_work(durations_and_admins, admins, labels_wealth, bins_wealth):
  admins['wealth_label'] = pd.cut(
      admins['pct_wealth'], bins_wealth, labels=labels_wealth)
  admins['geom_id'] = admins['geom_id'].astype(str)
  admins['wealth_label'] = admins['wealth_label'].astype(str)
  # get admin info for home and work location
  tmp1 = spark.createDataFrame(
      admins[['geom_id', 'pop', 'pct_wealth', 'wealth_label']].rename(columns=lambda x: x+'_home'))
  tmp2 = spark.createDataFrame(
      admins[['geom_id', 'pct_wealth', 'wealth_label']].rename(columns=lambda x: x+'_work'))
  out1 = (durations_and_admins
          .join(tmp1, on='geom_id_home', how='inner')
          .join(tmp2, on='geom_id_work', how='inner'))

  geom_users = (out1
                .groupby('geom_id_home')
                .agg(F.countDistinct('user_id').alias('n_users')))

  out = (out1
         .join(geom_users, on='geom_id_home', how='inner')
         .withColumn('weight', col('pop_home')/col('n_users')))
  return out


def output_hw(out, column):
  # compute aggregate measures
  out = (out
         .fillna(0, subset=column)
         .groupby('date', 'wealth_label_home', 'wealth_label_work')
         .agg((F.sum(col(column)*col('weight'))/F.sum(col('weight'))).alias('mean'),
              F.stddev(column).alias('std'),
              F.count(column).alias('n'),
              F.countDistinct('user_id').alias('n_unique'))
         .withColumn('sem', col('std')/F.sqrt(col('n')))
         .drop('std'))

  durations_normalized_by_wealth_home_wealth_work = out.toPandas(
  ).set_index(['wealth_label_home', 'wealth_label_work', 'date'])
  return durations_normalized_by_wealth_home_wealth_work


def plot_results(axes, row, column, indicator, country, data, labels_wealth, start_date, end_date, ma=28):
  data = data.sort_index(level='date')
  for k, wealth_label_home in enumerate(labels_wealth):
      if 'hw' in indicator:
          city_wealth = data[data['wealth_label_work']
                             == labels_wealth[k]].loc[wealth_label_home]
      else:
          city_wealth = data.loc[wealth_label_home]
      city_wealth = google_change_metric(city_wealth, start_baseline, end_baseline)
      city_wealth = city_wealth.loc[start_date:end_date]
      x2 = city_wealth.index
      y2 = city_wealth['mean'].rolling(ma, center=True, min_periods=1).mean()
      y2err = city_wealth['sem'].rolling(
          ma, center=True, min_periods=1).mean()
      axes[row, column].plot(x2, y2, linewidth=1,
                             color=[sns.color_palette("Paired")[1], sns.color_palette("Paired")[
                                 3], sns.color_palette("Paired")[5]][k],
                             label=wealth_label_home)
      axes[row, column].tick_params(which='both', direction='in', pad=3)
      axes[row, column].locator_params(axis='y', nbins=8)
      axes[row, column].set_ylabel(ylabels[indicator], fontweight='bold')
      min_max = list(zip(axes[row, column].get_ylim(), (y2.min().min(
      )-np.abs(y2.min().min()/5), y2.max().max()+np.abs(y2.max().max()/5))))
#       axes[row, column].set_ylim((np.min(min_max), np.max(min_max)))

      axes[row, column].fill_between(x2, y2-2*y2err, y2+2*y2err,
                                     alpha=0.1, color=[sns.color_palette("Paired")[1], sns.color_palette("Paired")[3], sns.color_palette("Paired")[5]][k])
      if indicator == 'comms_hw':
          axes[row, column].set_title(
              'Users living in low wealth admin. units in ' + country, fontweight='bold')
      else:
          axes[row, column].set_title(
              'Users living in ' + country, fontweight='bold')
      axes[row, column].legend(title=['Wealth of home admin. unit',
                                      'Wealth of workplace admin. unit'][1 if 'hw' in indicator else 0])


def read_admin(country):
  admin_path = f'/mnt/Geospatial/admin/{country}/admin.csv'
  admin = spark.read.option('header', 'true').csv(admin_path)
  admin = (admin
           .withColumn('urban/rural', F.when(col('metro_area_name').isNull(), lit('rural')).otherwise(lit('urban')))
           .select('geom_id', 'urban/rural'))
  return admin


def compute_rural_migration_stats_city(country, bins_wealth, labels_wealth, hw, ww, wa, mph, mpw, activity_level, c_dates,admin_path,stop_path):
  admins, admins_by_metro_area, pops = process_admin(country,admin_path)
  admins['geom_id'] = admins['geom_id'].astype(str)

  admins_by_metro_area['wealth_label'] = pd.cut(admins_by_metro_area['pct_wealth'], bins_wealth, labels=labels_wealth)
  admins_by_metro_area['geom_id'] = admins_by_metro_area['geom_id'].astype(str)
  admins_by_metro_area['wealth_label'] = admins_by_metro_area['wealth_label'].astype(str)

  admins_by_metro_area = admins_by_metro_area.loc[admins_by_metro_area.metro_area_name == pops.reset_index().metro_area_name[0]]
  admins = spark.createDataFrame(admins_by_metro_area).select('geom_id', 'wealth_label')

  admin_rural = read_admin(country)

  # get list of active users
  fname_nf = f'durations_window_hw{hw}_ww{ww}_wa{wa}_mph{mph}_mpw{mpw}'
  durations_path_nf = f'{stop_path}{country}/accuracy100_maxtimestop3600_staytime300_radius50_dbscanradius50/date{c_dates[country]}/' + fname_nf
  durations = spark.read.parquet(durations_path_nf)
  active_users = get_active_list(durations, country, activity_level)

  # read stops, filter actives, get most frequented daily geom id, and get rural/urban info
  personal_nf = f"personal_stop_location_hw{hw}_ww{ww}_wa{wa}_mph{mph}_mpw{mpw}"
  stops = spark.read.parquet(f"{stop_path}{country}/accuracy100_maxtimestop3600_staytime300_radius50_dbscanradius50/date{c_dates[country]}/" + personal_nf)

  admins_by_country, admins_by_metro_area, pop_metro_areas = process_admin(country,admin_path)
  metro = (admins_by_metro_area
           .loc[admins_by_metro_area.metro_area_name == pop_metro_areas
                .reset_index()
                .head(1)
                .metro_area_name
                .to_list()[0]]["geom_id"]
           .to_list())

  users_metro = stops.where(col('location_type') == 'H').filter(
      F.col("geom_id").isin(metro)).select('user_id').distinct()
  stops = stops.join(users_metro, on='user_id', how='inner').join(
      active_users, on='user_id', how='inner')

  w = Window.partitionBy('user_id')
  user_geom = (stops
               .where(col('location_type') == 'H')
               .filter(F.col("geom_id").isin(metro))
               #                .where(col('date_trunc') <= '2020-03-15')
               .groupby('user_id', 'geom_id')
               .agg(F.countDistinct('date_trunc').alias('n_days'))
               .withColumn('max_days', F.max('n_days').over(w))
               .where(col('n_days') == col('max_days'))
               .groupby('user_id')
               .agg(F.first('geom_id').alias('geom_id'))
               .join(admins, on='geom_id')
               .drop('geom_id'))

  usrs = (user_geom
          .join(active_users, on='user_id', how='inner')
          .toPandas())
  usrs["country"] = country

  w = Window.partitionBy('user_id', 'date_trunc')
  h_stops = (stops
             .where(col('location_type') == 'H')
             .join(active_users, on='user_id', how='inner')
             .groupby('user_id', 'date_trunc', 'geom_id')
             .agg(F.sum('duration').alias('duration'))
             .withColumn('max_duration', F.max('duration').over(w))
             .where(col('duration') == col('max_duration'))
             .groupby('user_id', 'date_trunc')
             .agg(F.first('geom_id').alias('geom_id'))
             .join(admin_rural, on='geom_id', how='inner')
             .join(user_geom, on='user_id', how='inner'))

  # look-up previous geom id to identify migrations with direction
  w = Window.partitionBy('user_id').orderBy('date_trunc')
  h_stops = (h_stops
             .withColumn('prev_geom_id', F.lag('geom_id', offset=1).over(w))
             .withColumn('prev_urban/rural', F.lag('urban/rural', offset=1).over(w))
             .withColumn('prev_date', F.lag('date_trunc', offset=1).over(w))
             .where(col('prev_geom_id').isNotNull())
             .withColumn('change', F.when(col('urban/rural') == col('prev_urban/rural'), 'no change')
                                    .otherwise(F.when(col('urban/rural') == 'urban', 'rural to urban')
                                                .otherwise(F.when(col('urban/rural') == 'rural', 'urban to rural'))))
             .withColumn('gap', F.datediff(col('date_trunc'), col('prev_date')))
             .withColumn('rand_gap', (-1*F.rand()*(col('gap')-1)).astype(IntegerType()))
             .withColumn('new_date', F.expr("date_add(date_trunc, rand_gap)"))
             .withColumn('date_trunc', col('new_date')))
  # .withColumn('date_trunc', F.when(col('gap') > 30, col('new_date')).otherwise(col('date_trunc'))))

  # aggregate by day and change and return as pandas df
  out = (h_stops
         .groupby('date_trunc', 'wealth_label', 'change')
         .agg(F.countDistinct('user_id').alias('n_users'))
         .withColumnRenamed('date_trunc', 'date')
         .toPandas())
  out['date'] = pd.to_datetime(out['date'])
  out['country'] = country
  return out, usrs

def compute_rural_migration_stats(country, hw, ww, wa, mph, mpw, c_dates,stop_path):
  # read admin
  admin = read_admin(country)

  # get list of active users
  fname_nf = f'durations_window_hw{hw}_ww{ww}_wa{wa}_mph{mph}_mpw{mpw}'
  durations_path_nf = f'{stop_path}{country}/accuracy100_maxtimestop3600_staytime300_radius50_dbscanradius50/date{c_dates[country]}/' + fname_nf
  durations = spark.read.parquet(durations_path_nf)
  active_users = get_active_list(durations, country, activity_level)
  admins_by_country, admins_by_metro_area, pop_metro_areas = process_admin(country,admin_path)
  metro = (admins_by_metro_area
           .loc[admins_by_metro_area.metro_area_name == pop_metro_areas
                .reset_index()
                .head(1)
                .metro_area_name
                .to_list()[0]]["geom_id"]
           .to_list())

  # read stops, filter actives, get most frequented daily geom id, and get rural/urban info
  personal_nf = f"personal_stop_location_hw{hw}_ww{ww}_wa{wa}_mph{mph}_mpw{mpw}"
  stops = spark.read.parquet(
      f"{stop_path}{country}/accuracy100_maxtimestop3600_staytime300_radius50_dbscanradius50/date{c_dates[country]}/" + personal_nf)
  users_metro = stops.where(col('location_type') == 'H').filter(
      F.col("geom_id").isin(metro)).select('user_id').distinct()
  stops = stops.join(users_metro, on='user_id', how='inner').join(
      active_users, on='user_id', how='inner')

  w = Window.partitionBy('user_id', 'date_trunc')
  h_stops = (stops
             .where(col('location_type') == 'H')
             .join(active_users, on='user_id', how='inner')
             .groupby('user_id', 'date_trunc', 'geom_id')
             .agg(F.sum('duration').alias('duration'))
             .withColumn('max_duration', F.max('duration').over(w))
             .where(col('duration') == col('max_duration'))
             .groupby('user_id', 'date_trunc')
             .agg(F.first('geom_id').alias('geom_id'))
             .join(admin, on='geom_id', how='inner'))

  # look-up previous geom id to identify migrations with direction
  w = Window.partitionBy('user_id').orderBy('date_trunc')
  h_stops = (h_stops
             .withColumn('prev_geom_id', F.lag('geom_id', offset=1).over(w))
             .withColumn('prev_urban/rural', F.lag('urban/rural', offset=1).over(w))
             .withColumn('prev_date', F.lag('date_trunc', offset=1).over(w))
             .where(col('prev_geom_id').isNotNull())
             .withColumn('change', F.when(col('urban/rural') == col('prev_urban/rural'), 'no change')
                                    .otherwise(F.when(col('urban/rural') == 'urban', 'rural to urban')
                                                .otherwise(F.when(col('urban/rural') == 'rural', 'urban to rural'))))
             .withColumn('gap', F.datediff(col('date_trunc'), col('prev_date')))
             .withColumn('rand_gap', (-1*F.rand()*(col('gap')-1)).astype(IntegerType()))
             .withColumn('new_date', F.expr("date_add(date_trunc, rand_gap)"))
             .withColumn('date_trunc', F.when(col('gap') > 30, col('new_date')).otherwise(col('date_trunc'))))

  # aggregate by day and change and return as pandas df
  out = (h_stops
         .groupby('date_trunc', 'change')
         .agg(F.countDistinct('user_id').alias('n_users'))
         .withColumnRenamed('date_trunc', 'date')
         .toPandas())
  out['date'] = pd.to_datetime(out['date'])
  return out

def get_migration_results(results,users,state,frac='net_rural_fraction',change=True,cumulated=True):
  tmp = results[results['country'] == state].copy()
  tmp = tmp.loc[tmp["date"] < pd.to_datetime(c_dates[state])]
  tmp = tmp.sort_values(by=['wealth_label', 'change', 'date'])
  tmp['tot_users'] = tmp.groupby(['wealth_label'])['n_users'].transform(sum)
  tmp['users_fraction'] = tmp['n_users']/tmp['tot_users']*100 # not in use
  tmp['n_users_cumsum'] = tmp.groupby(['wealth_label', 'change'])['n_users'].cumsum()

  usrs = users.loc[users.country == state].copy()
  wl_users = usrs.groupby('wealth_label')['user_id'].nunique().reset_index()
  cumsum = tmp[['date', 'wealth_label', 'change', 'n_users_cumsum']].pivot_table(index=['wealth_label', 'date'], columns='change', values='n_users_cumsum').reset_index()
#   if country == "ZA":
  cumsum = cumsum.bfill().ffill()
  cumsum['net_rural'] = cumsum['urban to rural'] - cumsum['rural to urban']
  cumsum = pd.merge(cumsum, wl_users, on='wealth_label')
  cumsum['net_rural_fraction'] = cumsum['net_rural']/cumsum['user_id']*100
  res = cumsum.loc[cumsum["date"] < pd.to_datetime(c_dates[state])]

  if change:
    res = base_diff_metric(res, frac, start_baseline,end_baseline,other_groups=['wealth_label'])
  else: 
    res = res.rename(columns={frac:'mean'}).copy()
  res = res.reset_index().pivot(index='date',columns='wealth_label',values='mean')
  res['state'] = state
  return res
  
  
### Analysis plots: Mobility
%matplotlib inline
%config InlineBackend.figure_format = 'retina'

def get_single_metric_results(results,metric,state,other_groups=[]):
  if not '_hw' in metric:
    res = results[(results['state']==state)&(results['measure']==metric)].set_index('date')
    res = google_change_metric(res, start_baseline, end_baseline,other_groups=other_groups).reset_index()
    rm = res.pivot(index='date',columns='wealth_label_home',values='mean')
    re = res.pivot(index='date',columns='wealth_label_home',values='sem') 
  else:
    res = results[(results['state']==state)&(results['measure']==metric)&(results['wealth_label_home']==labels_wealth[0])].set_index('date')
    res = google_change_metric(res, start_baseline, end_baseline,other_groups=other_groups).reset_index()
    rm = res.pivot(index='date',columns='wealth_label_work',values='mean')
    re = res.pivot(index='date',columns='wealth_label_work',values='sem') 
  return rm, re, (rm[(rm.index>=start_date)&(rm.index<=end_date)].rolling(ma).mean().min().min(),rm[(rm.index>=start_date)&(rm.index<=end_date)].rolling(ma).mean().max().max())
  
def set_plot_style(ax, state, xlim=None, ylim=None, title=None, xlabel=None, ylabel=None, byweekday=0, add_important_dates=True, weeks_interval=2, fs=10):
  if title:
      ax.set_title(title,fontsize=fs+2)
  if xlabel:
      ax.set_xlabel(xlabel, fontsize=fs)
  if ylabel:
      ax.set_ylabel(ylabel, fontsize=fs)
  if xlim:
      ax.set_xlim(xlim)
  if ylim:
      ax.set_ylim(ylim)

  # set ticks every week
  ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=weeks_interval, byweekday=(byweekday)))
  # set major ticks format
  ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
  plt.setp(ax.get_xticklabels(), rotation=45, ha='right',fontsize=fs)
  plt.setp(ax.get_yticklabels(),fontsize=fs)
  # remove spines
  ax.spines['top'].set_visible(False)
  ax.spines['right'].set_visible(False)
  # remove minor ticks
  ax.minorticks_off()
  ax.tick_params(width=0.5)

import matplotlib as mpl
mpl.rcParams['axes.linewidth'] = 0.5
def plot_metrics(results,states, metrics = ['rec', 'comms', 'comms_hw'], # ['t_home', 't_work', 't_other', 'rec', 'comms', 'comms_hw']
                 color_n = [1,2,6], cols=3, fs = 6, lw = 1.1, other_groups = []):
  alph = list(string.ascii_lowercase)
  xlims = (pd.to_datetime(start_date),pd.to_datetime(end_date))
  lm = len(metrics)
  ls = len(states)
  fig, axes = plt.subplots((lm*ls)//cols, cols, figsize=(8,2*(lm*ls)//cols))

  for sc,state in enumerate(states):
    for nm,metric in enumerate(metrics):
      i = (sc)
      j = (nm)
      # to normalize change independently (group-per-group) add a list of columns to use as an additional grouper!
      ylims = [get_single_metric_results(results,metric,state_temp,other_groups=other_groups) for state_temp in countries]
      ymin = [y[2][0] for y in ylims]
      ymax = [y[2][1] for y in ylims]
  #     ylims = [min(ymin)-0.1,max(ymax)+0.1]
      ylims = None

      rm, re, _ = get_single_metric_results(results,metric,state,other_groups=other_groups)
      ### smoothing curves
      rm = rm.rolling(ma,center=True).mean()
      rm = rm[(rm.index>=xlims[0])&(rm.index<=xlims[1])]
      re = re.rolling(ma,center=True).mean()
      re = re[(re.index>=xlims[0])&(re.index<=xlims[1])]

      for k,col in enumerate(rm.columns):
        if (lm*ls)>3:
          axes[i,j].plot(rm[col].index,rm[col],color=my_palette[color_n[k]],label=col.replace('_',' '))
          axes[i,j].fill_between(rm[col].index,rm[col]+2*re[col],rm[col]-2*re[col],color=my_palette[color_n[k]],alpha=0.1,linewidth=lw)
        else:
          axes[j].plot(rm[col].index,rm[col],color=my_palette[color_n[k]],label=col.replace('_',' '))
          axes[j].fill_between(rm[col].index,rm[col]+2*re[col],rm[col]-2*re[col],color=my_palette[color_n[k]],alpha=0.1,linewidth=lw)
      if (lm*ls)>3:
        set_plot_style(axes[i,j], state, ylim=ylims, xlabel=' ', ylabel='Change {} (%)'.format(ylabels[metric]), byweekday=5,fs=fs, weeks_interval=8)
        axes[i,j].legend(frameon=False,fontsize=fs-2)
        axes[i,j].text(x=-.05,y=1.05,s=alph[cols*sc+nm]+')  {} - {}'.format(state,metric),transform=axes[i,j].transAxes,fontweight='semibold',fontsize=fs)        
      else:
        set_plot_style(axes[j], state, ylim=ylims, xlabel=' ', ylabel='Change {} (%)'.format(ylabels[metric]), byweekday=5,fs=fs, weeks_interval=8)
        axes[j].legend(frameon=False,fontsize=fs-2)
        axes[j].text(x=-.05,y=1.05,s=alph[(cols)*sc+nm]+')  {} - {}'.format(state,metric),transform=axes[j].transAxes,fontweight='semibold',fontsize=fs)

  plt.subplots_adjust(hspace=0.5,wspace=0.4)
  return fig


### Analysis plots: Migration
def plot_migration(results,states, color_n = [1,2,6], cols=3, fs = 6, lw = 1.1):
  alph = list(string.ascii_lowercase)
  xlims = (pd.to_datetime(start_date),pd.to_datetime(end_date))
  lm = len(states)
  fig, axes = plt.subplots(max(1,lm//cols), min(cols,lm), figsize=(min(8,3*lm),2*max(1,lm//cols)))
  
  ylims = results[(results.index>=start_date)&(results.index<=end_date)].sort_index().groupby('state').rolling(mw,center=True,min_periods=1).mean()
#   ylims = [ylims.min().min()-0.01,ylims.max().max()+0.01]
  ylims = None

  for nm,state in enumerate(states):
    i = nm//cols
    j = nm%cols
    # to normalize change independently (group-per-group) add a list of columns to use as an additional grouper!
    r = results[(results['state']==state)&(results.index<=end_date)].drop(columns=['state'])
    ### smoothing curves
    rm = r.rolling(mw,center=True,min_periods=1).mean()
    re = r.rolling(mw,center=True,min_periods=1).sem()
    rm = rm[(rm.index>=xlims[0])&(rm.index<=xlims[1])]
    re = re[(re.index>=xlims[0])&(re.index<=xlims[1])]

    for k,col in enumerate(rm.columns):
      if lm>cols:
        axes[i,j].plot(rm[col].index,rm[col],color=my_palette[color_n[k]],label=col.replace('_',' '))
        axes[i,j].fill_between(rm[col].index,rm[col]+2*re[col],rm[col]-2*re[col],color=my_palette[color_n[k]],alpha=0.1,linewidth=lw)
        axes[i,j].text(x=-.05,y=1.05,s=alph[nm*k]+')  {}'.format(state),transform=axes[i,j].transAxes,fontweight='semibold',fontsize=fs)
      elif lm>1:
        axes[j].plot(rm[col].index,rm[col],color=my_palette[color_n[k]],label=col.replace('_',' '))
        axes[j].fill_between(rm[col].index,rm[col]+2*re[col],rm[col]-2*re[col],color=my_palette[color_n[k]],alpha=0.1,linewidth=lw)
        axes[i].text(x=-.05,y=1.05,s=alph[nm]+')  {}'.format(state),transform=axes[i].transAxes,fontweight='semibold',fontsize=fs)
      else:
        axes.plot(rm[col].index,rm[col],color=my_palette[color_n[k]],label=col.replace('_',' '))
        axes.fill_between(rm[col].index,rm[col]+2*re[col],rm[col]-2*re[col],color=my_palette[color_n[k]],alpha=0.1,linewidth=lw)
        axes.text(x=-.05,y=1.05,s=alph[nm]+')  {}'.format(state),transform=axes.transAxes,fontweight='semibold',fontsize=fs)
 
    if lm>cols:
      set_plot_style(axes[i,j], state, ylim=ylims, xlabel=' ', ylabel='Urban to rural\nmigration change', byweekday=5,fs=fs, weeks_interval=8)
      axes[i,j].legend(frameon=False,fontsize=fs-2)
    elif lm>1:
      set_plot_style(axes[j], state, ylim=ylims, xlabel=' ', ylabel='Urban to rural\nmigration change', byweekday=5,fs=fs, weeks_interval=8)
      axes[j].legend(frameon=False,fontsize=fs-2)
    else:
      set_plot_style(axes, state, ylim=ylims, xlabel=' ', ylabel='Urban to rural\nmigration change', byweekday=5,fs=fs, weeks_interval=8)
      axes.legend(frameon=False,fontsize=fs-2)

  plt.subplots_adjust(hspace=0.5,wspace=0.4)
  return fig

my_palette = sns.color_palette(['#ce343c','#EC8A61', '#89d4b4','#0081A7', '#72C9C8', '#547474', '#8a567a', '#743D55','#b25a97', '#efb953'])
