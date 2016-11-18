from pandas import read_csv, read_json, DataFrame
from pandas.io.json import json_normalize
import time
from datetime import date, timedelta
import json
from urllib import urlretrieve

def results():
    urlretrieve('https://interactives.ap.org/interactives/2016/general-election/live-data/production/2016-11-08/president.json', 'president.json')

    with open('president.json') as input_file:
        json_data = json.load(input_file)

    data = json_normalize(json_data['results'])

    row_count = len(data)
    row_index = 0
    df = DataFrame(columns=['id','state','candidate','vote','vote_pct'])

    while row_index < row_count:
        candidate_count = len(data.ix[row_index]['cand'])
        candidate_index = 0
        while candidate_index < candidate_count:
            df.loc[len(df), 'id'] = data.ix[row_index]['id']
            df.loc[len(df) - 1, 'state'] = data.ix[row_index]['st']
            df.loc[len(df) - 1, 'candidate'] = data.ix[row_index]['cand'][candidate_index]
            df.loc[len(df) - 1, 'vote'] = data.ix[row_index]['vote'][candidate_index]
            df.loc[len(df) - 1, 'vote_pct'] = data.ix[row_index]['vp'][candidate_index]
            candidate_index += 1
        row_index += 1

    df = df[['id','state','candidate','vote','vote_pct']]

    return df


def mapping():
    urlretrieve('http://interactives.ap.org/interactives/2016/general-election/live-data/production/2016-11-08/president_metadata.json', 'president_metadata.json')
    with open('president_metadata.json') as input_file:
        json_data = json.load(input_file)

    json_data_to_dict = json.dumps(json_data)
    item_dict = json.loads(json_data_to_dict)

    df_transposed = DataFrame.from_dict(item_dict['cands'])
    df = df_transposed.transpose()
    df.reset_index(inplace=True)
    df.columns=['id','candidate','party']

    return df

def set_state_name(x):
    if x[0] == 'county':
        return ''
    else:
        return x[1]

def set_name(x):
    if x[0] == '':
        return x[1]
    else:
        return x[0]

def merge_results_mapping(results, mapping):
    merged = results.merge(mapping, left_on='candidate', right_on='id')
    merged_df = merged[['id_x','state','candidate_y','party','vote','vote_pct']]
    merged_df.columns = ['id','state','candidate','party','votes','votes_pct']
    merged_df['type'] = merged_df['id'].apply(lambda x: 'county' if x.isnumeric() else 'national' if x=='US' else 'state')
    merged_df['state_national_name'] = merged_df[['type','state']].apply(set_state_name, axis=1)
    return merged_df

def county_name(input_df):
    names = read_csv('fips.csv')
    names['county_fips'] = names['county_fips'].apply(lambda x: '{0:0>5}'.format(x))
    names['state_fips'] = names['state_fips'].apply(lambda x: '{0:0>2}'.format(x))
    df = input_df.merge(names, how='left', left_on='id', right_on='county_fips')
    df['name'] = df[['state_national_name','county']].apply(set_name, axis=1)
    df['name'].fillna('', inplace=True)
    out = df[['id','state_x','name','type','candidate','party','votes','votes_pct']]
    out.columns = ['id','state','name','type','candidate','party','votes','votes_pct']
    return out

if __name__ == '__main__':
    results = results()
    mapping = mapping()

    merged = merge_results_mapping(results, mapping)
    out = county_name(merged)

    out.to_csv('presidential_election_results.csv', index=False)
