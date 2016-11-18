from pandas import read_json, DataFrame
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

if __name__ == '__main__':
    results = results()
    mapping = mapping()
    merged = results.merge(mapping, left_on='candidate', right_on='id')
    out = merged[['id_x','state','candidate_y','party','vote','vote_pct']]
    out.columns = ['id','state','candidate','party','votes','votes_pct']
    out['type'] = out['id'].apply(lambda x: 'county' if x.isnumeric() else 'national' if x=='US' else 'state')

    out.to_csv('presidential_election_results.csv', index=False)