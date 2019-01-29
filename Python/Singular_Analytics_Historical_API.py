import copy
import requests
import datetime

API_KEY = ''
UPDATE_ENDPOINT = '/api/last_modified_dates?'
REPORT_ENDPOINT = '/api/v2.0/reporting?'
BASE_REPORTING_URL = 'https://api.singular.net'

update_params = {
    'api_key': API_KEY,
    'timestamp': (datetime.date.today() - datetime.timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S"),
    'group_by_source': True
}

params = {
    'api_key': API_KEY,
    'format': 'json',
    'cohort_metrics': 'revenue,roi',
    'start_date': '',
    'end_date': '',
    'cohort_periods': '7d,14d',
    'dimensions': 'app,source,os',
    'discrepancy_metrics': 'adn_clicks,tracker_clicks,adn_installs,tracker_installs',
    'metrics': 'custom_clicks,custom_installs,adn_cost',
    'time_breakdown': 'all',
    'country_code_format': 'iso3'
}


def query(params_separated_by_day):
    def format_custom_metrics(record):
        new_dict = {}
        keys_to_delete = []
        for k, v in record.iteritems():
            if isinstance(v, dict):
                for kk, vv in v.iteritems():
                    new_key = "%s_%s" % (k, kk)
                    new_dict.update({new_key: vv})
                keys_to_delete.append(k)
        if keys_to_delete:
            for key in keys_to_delete:
                del record[key]
        record.update(new_dict)
        return record

    joined_results = []
    for single_day_params in params_separated_by_day:
        result = requests.request('GET', BASE_REPORTING_URL + REPORT_ENDPOINT, params=single_day_params)
        if result and result.json().get('value', {}).get('results', {}):
            for record in result.json()['value']['results']:
                record = format_custom_metrics(record)
                joined_results.append(record)
    return joined_results


def build_params(network, data):
    params_separated_by_day = []
    for single_day in data:
        single_day_params = copy.deepcopy(params)
        single_day_params['end_date'] = single_day_params['start_date'] = single_day
        single_day_params['source'] = network
        params_separated_by_day.append(single_day_params)
    return params_separated_by_day


def get_adnework_updates():
    result = requests.request('GET', BASE_REPORTING_URL + UPDATE_ENDPOINT, params=update_params)
    if result and result.json().get('value', {}).get('results', {}):
        return result.json()['value'].get('results')


def get_data_for_update():
    data = get_adnework_updates()
    results = []
    for network in data:
        print "Building params for: ", network
        params_separated_by_day = build_params(network, data[network])
        print "Creating data for update to database..."
        results += query(params_separated_by_day)
    return results

data_to_update = get_data_for_update()