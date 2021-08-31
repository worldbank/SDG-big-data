import pandas as pd
from wbgps import compute_rural_migration_stats_city

start_baseline = '2020-02-01'
end_baseline = '2020-03-01'
start_date = '2020-02-01'
end_date = '2021-04-15'

results_dir = '/dbfs/mnt/Geospatial/results/veraset/'
admin_path = f'/mnt/Geospatial/admin/'  # country specific admin files should be stored as "{admin_path}/{country}/admin.csv"
stop_path = '/mnt/Geospatial/results/veraset/'

bins_wealth = [0, 0.4, 0.8, 1]
bins_dist = [0, 0.4, 0.8, 1]

countries = ['ID', 'PH', 'BR', 'CO', 'MX', 'ZA']
# countries = ['PH']

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

# Smoothing size for mobility
ma = 28
# Smoothing size for migrations
mw = 28

labels_wealth = [x + ' (' + str(int(bins_wealth[i] * 100)) + '%-' + str(int(bins_wealth[i + 1] * 100)) + '%)' for i, x
                 in zip(range(len(bins_wealth) - 1), ['Low', 'Medium', 'High'])]
labels_dist = [x + ' (' + str(int(bins_dist[i] * 100)) + '%-' + str(int(bins_dist[i + 1] * 100)) + '%)' for i, x in
               zip(range(len(bins_dist) - 1), ['Low', 'Medium', 'High'])]
country_capitals = {'MX': 'Mexico City', 'BR': 'São Paulo', 'CO': 'Bogota',
                    'PH': 'Quezon City [Manila]', 'ID': 'Jakarta', 'ZA': 'Johannesburg', 'AR': 'GBuenos Aires'}

### Comute migration per socio economic group
df_wl = pd.DataFrame()
usr_df = pd.DataFrame()
for country in countries:
    print(country)
    out, usrs = compute_rural_migration_stats_city(country, bins_wealth, labels_wealth, hw, ww, wa, mph, mpw,
                                                   activity_level, c_dates, admin_path, stop_path)
    usr_df = pd.concat([usr_df, usrs])
    df_wl = pd.concat([df_wl, out], sort=True)

df_wl.to_csv(results_dir + 'df_wl_migration_weighted.csv')
usr_df.to_csv(results_dir + 'df_usr_migration_weighted.csv')
