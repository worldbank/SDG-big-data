import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import seaborn as sns

import pyspark.sql.functions as F
from pyspark.sql.functions import col, desc, lit
from pyspark.sql import Window

spark.conf.set("spark.sql.shuffle.partitions", 1500)

start_baseline = '2020-02-01'
end_baseline = '2020-03-01'
start_date = '2020-02-01'
end_date = '2021-04-15'

results_dir = '/dbfs/mnt/Geospatial/results/veraset/'
admin_path = f'/mnt/Geospatial/admin/' # country specific admin files should be stored as "{admin_path}/{country}/admin.csv"
stop_path = '/mnt/Geospatial/results/veraset/'


bins_wealth = [0, 0.4, 0.8, 1]
bins_dist = [0, 0.4, 0.8, 1]

countries = ['ID', 'PH', 'BR', 'CO', 'MX', 'ZA']


c_dates = {'BR': '2021-05-16',
           'CO': '2021-05-16',
           'ID': '2021-05-16',
           'MX': '2021-05-16',
           'PH': '2021-05-16',
           'ZA': '2021-05-16',
           'AR': '2021-05-16'}

activity_level = 0.2

hw = 49
ww = 49
wa = 3600
mph = 0.2
mpw = 0.2

ma = 28


labels_wealth = [x+' ('+str(int(bins_wealth[i]*100))+'%-'+str(int(bins_wealth[i+1]*100)) +'%)' for i, x in zip(range(len(bins_wealth)-1), ['Low', 'Medium', 'High'])]
labels_dist = [x+' ('+str(int(bins_dist[i]*100))+'%-'+str(int(bins_dist[i+1]*100)) +'%)' for i, x in zip(range(len(bins_dist)-1), ['Low', 'Medium', 'High'])]
country_capitals = {'MX': 'Mexico City', 'BR': 'São Paulo', 'CO': 'Bogota', 'PH': 'Quezon City [Manila]', 'ID': 'Jakarta', 'ZA': 'Johannesburg', 'AR': 'GBuenos Aires'}


### Compute mobility metrics
results = {}
for country in countries:
    print(country)

    admins_by_country, admins_by_metro_area, pop_metro_areas = process_admin(country,admin_path)
    capital_geomid = admins_by_metro_area[admins_by_metro_area.metro_area_name == country_capitals[country]].geom_id.unique().tolist()
    durations_and_admins = compute_durations_and_admins(country, c_dates[country], stop_path, activity_level=activity_level, hw=hw, ww=ww, wa=wa, mph=mph, mpw=mpw)
    durations_and_admins.cache()
    out1 = compute_durations_normalized_by_wealth_home(durations_and_admins, admins_by_country, labels_wealth, bins_wealth)
    out2 = compute_durations_normalized_by_wealth_home_wealth_work(durations_and_admins, admins_by_country, labels_wealth, bins_wealth)
    results[(country, 't_home')] = output(out1, column='H')
    results[(country, 't_work')] = output(out1, column='W')
    results[(country, 't_other')] = output(out1, column='O')
    results[(country, 'rec')] = output(out1, column='R')
    results[(country, 'comms')] = output(out1, column='C')
    results[(country, 'comms_hw')] = output_hw(out2, column='C')
    results[(country, 't_work_hw')] = output_hw(out2, column='W')

    durations_and_admins.unpersist()
# save results
res2 = pd.DataFrame(columns=['state', 'measure',
                             'wealth_label_home', 'date', 'mean', 'sem'])
for key, res in results.items():
    res_tmp = res.reset_index().copy()
    res_tmp['state'], res_tmp['measure'] = key
    res2 = res2.append(res_tmp, ignore_index=True)

res2.to_csv(results_dir+'all_hw_weighted.csv')
