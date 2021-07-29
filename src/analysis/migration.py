from wbgps import *
import datetime
import seaborn as sns
from sklearn.neighbors import DistanceMetric
from pyspark.sql.functions import col, desc, lit, pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType, ArrayType, MapType, StructType, StructField, LongType, StringType, IntegerType
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import pyspark.sql.functions as F
import json
import numpy as np
import os
from itertools import product
import pandas as pd

spark.conf.set("spark.sql.shuffle.partitions", 500)


bins_wealth = [0, 0.4, 0.8, 1]
labels_wealth = [x+' ('+str(int(bins_wealth[i]*100))+'%-'+str(int(bins_wealth[i+1]*100)) +
                 '%)' for i, x in zip(range(len(bins_wealth)-1), ['Low', 'Medium', 'High'])]

bins_dist = [0, 0.4, 0.8, 1]
labels_dist = [x+' ('+str(int(bins_dist[i]*100))+'%-'+str(int(bins_dist[i+1]*100)) +
               '%)' for i, x in zip(range(len(bins_dist)-1), ['Low', 'Medium', 'High'])]
print(labels_dist)

c_dates = {'BR': '2021-01-01',
           'CO': '2021-05-16',
           'ID': '2021-01-01',
           'MX': '2020-12-01',
           'PH': '2021-01-01',
           'ZA': '2021-05-16'}

hw=49
ww=49
wa=3600
mph=0.2
mpw=0.2

activity_level = 0.2

countries = ['BR', 'CO', 'ID', 'MX', 'PH', 'ZA']

df_wl = pd.DataFrame()
usr_df = pd.DataFrame()
for country in countries:
    print(country)
    out, usrs = compute_rural_migration_stats_city(country, bins_wealth, labels_wealth, hw, ww, wa, mph, mpw, activity_level, c_dates)
