import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from scipy.stats.mstats import winsorize
from timeit import default_timer as timer

# We'll import twitter data from this year range

yrange = range(2012,2021+1)

# List of names for different splits of the twitter accounts

split0 = ['gender','bin_n_days_since_first','bin_n_tweets_since_first']
splits = ['gender','ndays','ntweets'] # Abbreviated names for split0

# List of different twitter models

model0 = {}
model0['MX'] = [
        'keywords',
        'BERT_iter10_cutoff_proba_0.5',
        'BERT_iter10_cutoff_proba_0.9'
        ]
model0['BR'] = [
        'keywords',
        'BERT_iter9_cutoff_proba_0.5',
        'BERT_iter9_cutoff_proba_0.9'
        ]
model0['US'] = [
        'keywords',
        'BERT_iter8_cutoff_proba_0.5',
        'BERT_iter8_cutoff_proba_0.9'
        ]
models = ['keywords','bert05','bert09'] # Abbreviated names for model0

# List of countries

clist = ['MX','BR','US']

# List of variable names for twitter labels

labels = [
        'is_hired_1mo','is_unemployed',
        'job_offer','job_search','lost_job_1mo'
        ]
labels_w = [i+'_w' for i in labels] # Variable names for winsorized version

# Collect list of twitter-related variables

twitter_vars = ['n_users','n_users'+'_w'] + labels + labels_w

# Detailed names of account locations
# e.g. latitude/longitude, country, locality, admin. area

user_locations = pd.read_pickle('../../profiles/account-locations.pkl')

# Info about cities/metro areas

pop_metro_areas = {};
user_location_2_geo_id = {};
geo_id_2_metro_area_name = {};

for iso2 in clist:

    # Population for major cities

    filepath = '../../../metro_areas/' + iso2 + '/'

    pop_metro_areas[iso2] = pd.read_csv(
            filepath + '/' + 'pop_metro_areas' + '.csv', header = 0,
            usecols = ['year','geo_id','pop'])

    # user_location to geo_id and metro_area_name crosswalk

    user_location_2_geo_id[iso2] = pd.read_csv(
            filepath + 'user_location_2_geo_id' + '.csv', header = 0,
            usecols = ['user_location','geo_id','metro_area_name'])

    # geo_id to metro_area_name crosswalk

    geo_id_2_metro_area_name[iso2] = user_location_2_geo_id[iso2][
            ['geo_id','metro_area_name']
            ].drop_duplicates()

# Twitter Indices

def main():

    list_iso2 = {}; list_city = {}

    for iso2 in clist:
        for m in range(len(models)):

            for yy in yrange:

                filepath = '../../time_series_predictions/' + iso2 + '/'

                id_vars = ['year','month']

                print(filepath + model0[iso2][m] + '/' + str(yy))

                twitter = pq.read_table(
                        filepath + model0[iso2][m] + '/',
                        filters = [('year', '=', yy)])

                twitter = twitter.to_pandas()

                twitter.columns = (id_vars
                                   + ['user_location'] + splits
                                   + ['n_users'] + labels)

                user_locations_iso2 = user_locations.loc[
                        user_locations.country_short==iso2,
                        ['user_location','country_short']]

                twitter = pd.merge(
                        left = twitter, right = user_locations_iso2,
                        on = 'user_location', how = 'inner')

                # Winsorized number of labels tagged

                for i in ['n_users'] + labels:
                    twitter[i+'_w'] = winsorize(twitter[i], limits = [0.01,0.01])

                ### First: Aggregate to country level

                tmp_iso2 = twitter[id_vars+twitter_vars].groupby(id_vars).sum()

                # Log(n_users)

                tmp_iso2['ln_'+'n_users'] = np.log(tmp_iso2['n_users'])
                tmp_iso2['ln_'+'n_users'+'_w'] = np.log(tmp_iso2['n_users'+'_w'])

                # Labels as share (%) of n_users

                for i in labels:
                    tmp_iso2[i+'_'+'n_users'] = (
                            tmp_iso2[i] / tmp_iso2['n_users']
                            ) * 100
                    tmp_iso2[i+'_'+'n_users'+'_w'] = (
                            tmp_iso2[i+'_w'] / tmp_iso2['n_users'+'_w']
                            ) * 100

                # Add suffix to column names

                tmp_vars = [
                            i + '_' + models[m] + '_' + 'total'
                            for i in tmp_iso2.columns.difference(
                                    id_vars, sort = False)
                            ]

                tmp_iso2.columns = tmp_vars # id_vars

                ### Second: Aggregate to tmp_iso2 * group level

                for s in range(len(splits)):

                    tmp = twitter[id_vars+[splits[s]]+twitter_vars]
                    tmp = tmp.groupby(id_vars+[splits[s]], as_index = False).sum()

                    # Map deciles to High/Median/Low groups

                    if (splits[s] in ['ndays','ntweets']):

                        tmp.loc[tmp[splits[s]].isin([0,1,2]),('hml')] = 'L'
                        tmp.loc[tmp[splits[s]].isin([3,4,5,6]),('hml')] = 'M'
                        tmp.loc[tmp[splits[s]].isin([7,8,9,10]),('hml')] = 'H'

                        tmp = tmp.drop(splits[s],axis=1)
                        tmp = tmp.rename(columns={'hml':splits[s]})

                        tmp = tmp[id_vars+[splits[s]]+twitter_vars]

                    # Log(n_users)

                    tmp['ln_'+'n_users'] = np.log(tmp['n_users'])
                    tmp['ln_'+'n_users'+'_w'] = np.log(tmp['n_users'+'_w'])

                    # Labels as share (%) of n_users

                    for i in labels:
                        tmp[i+'_'+'n_users'] = (
                                tmp[i] / tmp['n_users_w']
                                ) * 100
                        tmp[i+'_'+'n_users'+'_w'] = (
                                tmp[i+'_w'] / tmp['n_users'+'_w']
                                ) * 100

                    # Add suffix to column names

                    tmp_vars = [
                            i + '_' + models[m] + '_' + splits[s]
                            for i in tmp.columns.difference(
                                    id_vars + [splits[s]], sort = False)
                            ]

                    tmp.columns = id_vars + [splits[s]] + tmp_vars

                    tmp[splits[s]] = tmp[splits[s]].astype(str)

                    # Reshape to wide format

                    tmp = tmp.pivot_table(
                            index = id_vars, columns = splits[s], values = tmp_vars,
                            aggfunc = np.sum, fill_value = 0)

                    tmp.columns = ['_'.join(col) for col in tmp.columns]
                    tmp = tmp.reset_index()

                    tmp_iso2 = pd.merge(
                            left = tmp_iso2, right = tmp,
                            on = id_vars, how = 'outer')

                list_iso2[iso2 + '_' + models[m] + '_' + str(yy)] = tmp_iso2

                ### Third: Aggregate to city level

                # Merge in City id (Drops obs without city id)

                id_vars = ['year','month','geo_id']

                twitter = pd.merge(
                        left = twitter, right = user_location_2_geo_id[iso2],
                        on = 'user_location', how = 'inner')

                tmp_city = twitter[id_vars+twitter_vars].groupby(id_vars).sum()

                # Log(n_users)

                tmp_city['ln_'+'n_users'] = np.log(tmp_city['n_users'])
                tmp_city['ln_'+'n_users'+'_w'] = np.log(tmp_city['n_users'+'_w'])

                # Labels as share (%) of n_users

                for i in labels:
                    tmp_city[i+'_'+'n_users'] = (
                            tmp_city[i] / tmp_city['n_users']
                            ) * 100
                    tmp_city[i+'_'+'n_users'+'_w'] = (
                            tmp_city[i+'_w'] / tmp_city['n_users'+'_w']
                            ) * 100

                # Add suffix to column names

                tmp_vars = [
                        i + '_' + models[m] + '_' + 'total'
                        for i in tmp_city.columns.difference(
                                id_vars, sort = False)
                        ]

                tmp_city.columns = tmp_vars # id_vars

                ### Second: Aggregate to tmp_city * group level

                for s in range(0,len(splits)):

                    tmp = twitter[id_vars+[splits[s]]+twitter_vars]
                    tmp = tmp.groupby(id_vars+[splits[s]], as_index = False).sum()

                    # Map deciles to High/Median/Low groups

                    if (splits[s] in ['ndays','ntweets']):

                        tmp.loc[tmp[splits[s]].isin([0,1,2,3]),('hml')] = 'L'
                        tmp.loc[tmp[splits[s]].isin([4,5,6]),('hml')] = 'M'
                        tmp.loc[tmp[splits[s]].isin([7,8,9,10]),('hml')] = 'H'

                        tmp = tmp.drop(splits[s],axis=1)
                        tmp = tmp.rename(columns={'hml':splits[s]})

                        tmp = tmp[id_vars+[splits[s]]+twitter_vars]

                    # Log(n_users)

                    tmp['ln_'+'n_users'] = np.log(tmp['n_users'])
                    tmp['ln_'+'n_users'+'_w'] = np.log(tmp['n_users'+'_w'])

                    # Labels as share (%) of n_users

                    for i in labels:
                        tmp[i+'_'+'n_users'] = (
                                tmp[i] / tmp['n_users_w']) * 100
                        tmp[i+'_'+'n_users'+'_w'] = (
                                tmp[i+'_w'] / tmp['n_users'+'_w']) * 100

                    # Add suffix to column names

                    tmp_vars = [
                            i + '_' + models[m] + '_' + splits[s]
                            for i in tmp.columns.difference(
                                    id_vars + [splits[s]], sort = False)
                            ]

                    tmp.columns = id_vars + [splits[s]] + tmp_vars

                    # Reshape to wide format

                    tmp[splits[s]] = tmp[splits[s]].astype(str)

                    tmp = tmp.pivot_table(
                            index = id_vars, columns = splits[s], values = tmp_vars,
                            aggfunc = np.sum, fill_value = 0)

                    tmp.columns = ['_'.join(col) for col in tmp.columns]
                    tmp = tmp.reset_index()

                    tmp_city = pd.merge(
                            left = tmp_city, right = tmp,
                            on = id_vars, how = 'outer')

                list_city[iso2 + '_' + models[m] + '_' + str(yy)] = tmp_city

        del twitter

    # For each country: Collect data from different sources

    data_iso2 = {}; data_city = {};

    for iso2 in clist:

        # Country-level Twitter data

        cnt0 = 1
        for m in range(len(models)):

            name_iso2m = iso2 + '_' + models[m]

            cnt1 = 1
            for yy in yrange:

                if cnt1==1:
                    list_iso2[name_iso2m] = list_iso2[name_iso2m + '_' + str(yy)]
                else:
                    list_iso2[name_iso2m] = list_iso2[name_iso2m].append(
                            list_iso2[name_iso2m + '_' + str(yy)])
                cnt1 = cnt1 + 1

            if cnt0==1:
                data_iso2[iso2] = list_iso2[name_iso2m]
            else:
                data_iso2[iso2] = pd.merge(
                        left = data_iso2[iso2],
                        right = list_iso2[name_iso2m],
                        on = ['year','month'], how = 'outer')
            cnt0 = cnt0 + 1

        # Country-level unemployment rate

        unrate_iso2 = pd.read_csv(
                '../../code/out/unrate_iso2_' + iso2 + '.csv', header = 0,
                usecols = ['unrate','year','month'])

        unrate_iso2.unrate = unrate_iso2.unrate * 100

        data_iso2[iso2] = pd.merge(
                left = data_iso2[iso2], right = unrate_iso2,
                on = ['year','month'], how = 'outer')

        data_iso2[iso2]['iso2'] = iso2
        data_iso2[iso2]['td'] = pd.to_datetime(
                data_iso2[iso2][['year','month']].assign(day = 1))
        data_iso2[iso2]['tm'] = pd.PeriodIndex(
                data_iso2[iso2]['td'], freq = 'M')
        data_iso2[iso2]['quarter'] = pd.DatetimeIndex(
                data_iso2[iso2]['td']).quarter

        data_iso2[iso2].to_csv('../out/data_iso2_' + iso2 + '.csv')

        # City-level Twitter data

        cnt0 = 1
        for m in range(len(models)):

            name_iso2m = iso2 + '_' + models[m]

            cnt1 = 1
            for yy in yrange:

                if cnt1==1:
                    list_city[name_iso2m] = list_city[name_iso2m + '_' + str(yy)]
                else:
                    list_city[name_iso2m] = list_city[name_iso2m].append(
                            list_city[name_iso2m + '_' + str(yy)])
                cnt1 = cnt1 + 1

            if cnt0==1:
                data_city[iso2] = list_city[name_iso2m]
            else:
                data_city[iso2] = pd.merge(
                        left = data_city[iso2],
                        right = list_city[name_iso2m],
                        on = ['year','month','geo_id'], how = 'outer')
            cnt0 = cnt0 + 1

        # City-level unemployment rate

        unrate_city = pd.read_csv(
                '../../code/out/unrate_city_' + iso2 + '.csv',
                header = 0, usecols = ['geo_id','unrate','year','month'])

        unrate_city.unrate = unrate_city.unrate * 100

        data_city[iso2] = pd.merge(
                left = data_city[iso2], right = unrate_city,
                on = ['geo_id','year','month'], how = 'outer')

        # City-level Population

        data_city[iso2] = pd.merge(
                left = data_city[iso2], right = pop_metro_areas[iso2],
                on = ['geo_id','year'], how = 'left')

        # City-level metro_area_name

        data_city[iso2] = pd.merge(
                left = data_city[iso2], right = geo_id_2_metro_area_name[iso2],
                on = ['geo_id'], how = 'left')

        data_city[iso2]['iso2'] = iso2
        data_city[iso2]['td'] = pd.to_datetime(
                data_city[iso2][['year','month']].assign(day = 1))
        data_city[iso2]['tm'] = pd.PeriodIndex(
                data_city[iso2]['td'], freq = 'M')
        data_city[iso2]['quarter'] = pd.DatetimeIndex(
                data_city[iso2]['td']).quarter

        data_city[iso2].to_csv('../out/data_city_' + iso2 + '.csv')


start = timer()

if __name__ == "__main__":
    main()

end = timer()

print()
print('Total Computing Time:', round(end - start), 'Sec')
