# ** Basic API Call **
import requests
import urllib

API_KEY = ''
ENDPOINT = '/api/v2.0/reporting?'
BASE_REPORTING_URL = 'https://api.singular.net{0}'.format(ENDPOINT)

params = {
    'api_key': API_KEY,
    'format': 'json',
    'cohort_metrics': 'revenue,roi',
    'start_date': '2017-09-25',
    'end_date': '2017-09-25',
    'cohort_periods': '7d,14d',
    'dimensions': 'app,source,os',
    'discrepancy_metrics': 'adn_clicks,tracker_clicks,adn_installs,tracker_installs',
    'metrics': 'custom_clicks,custom_installs,adn_cost',
    'time_breakdown': 'all',
    'country_code_format': 'iso3'
}

result = requests.request('GET', BASE_REPORTING_URL, params=params)
print result.json()


'''
** Multi-day API Call **
Query over multiple days and join data
Scenario: Developer needs to run a report that pulls data over multiple days and there are 100s of thousands of rows returning.
'''
import copy
import requests
import urllib

from datetime import datetime, timedelta

API_KEY = ''
ENDPOINT = '/api/v2.0/reporting?'
BASE_REPORTING_URL = 'https://api.singular.net{0}'.format(ENDPOINT)


def query(params):
    joined_results = []
    for single_day_params in params:
        result = requests.request('GET', BASE_REPORTING_URL, params=params)
        if result and result.json().get('value', {}).get('results', {}):
            for record in result.json()['value']['results']:
                joined_results.append(record)
    return joined_results


def parse_date_into_params(params):
    start_date = datetime.strptime(params['start_date'], '%Y-%m-%d').date()
    end_date = datetime.strptime(params['end_date'], '%Y-%m-%d').date()
    delta = (end_date - start_date).days + 1

    params_separated_by_day = []
    initial_start_date = start_date

    for single_day in xrange(delta):
        single_day_params = copy.deepcopy(params)
        single_day_date = (initial_start_date + timedelta(days=single_day))
        single_day_params['end_date'] = single_day_params['start_date'] = '{0}-{1}-{2}'.format(single_day_date.year, single_day_date.month, single_day_date.day)
        params_separated_by_day.append(single_day_params)
    return params_separated_by_day


if __name__ == "__main__":
    params = {
        'api_key': API_KEY,
        'format': 'json',
        'cohort_metrics': 'revenue,roi',
        'start_date': '2017-09-25',
        'end_date': '2017-09-26',
        'cohort_periods': '7d,14d',
        'dimensions': 'app,source,os',
        'discrepancy_metrics': 'adn_clicks,tracker_clicks,adn_installs,tracker_installs',
        'metrics': 'custom_clicks,custom_installs,adn_cost',
        'time_breakdown': 'all',
        'country_code_format': 'iso3'
    }

    list_of_params = parse_date_into_params(params)
    result = query(list_of_params)
    print result


'''
** Retrieve Custom Metrics API Call **
Get the Custom Metrics IDs to use in query
Scenario: Developer needs to get the Custom Mettics ID(s) to use within a specific report.
'''
import requests
import urllib


API_KEY = ''
ENDPOINT = '/api/cohort_metrics?'
BASE_REPORTING_URL = 'https://api.singular.net{0}'.format(ENDPOINT)

params = {
    'api_key': API_KEY
}

result = requests.request('GET', BASE_REPORTING_URL, params=params)
if result and result.json().get('value', {}).get('metrics', {}):
    metrics = result.json()['value']['metrics']
    if isinstance(metrics, list):
        for metric_name in metrics:
            print "Metrics Display Name: {0}\nID: {1}\n".format(metric_name['display_name'], metric_name['name'])


'''
** Retrieve Custom Dimensions API Call **
Get the Custom Dimension IDs to use in query
Scenario: Developer needs to get the Custom Dimension ID(s) to use within a specific report.
'''
import requests
import urllib

API_KEY = ''
ENDPOINT = '/api/custom_dimensions?'
BASE_REPORTING_URL = 'https://api.singular.net{0}'.format(ENDPOINT)

params = {
    'api_key': API_KEY
}

result = requests.request('GET', BASE_REPORTING_URL, params=params)
if result and result.json().get('value', {}).get('custom_dimensions', {}):
    dimensions = result.json()['value']['custom_dimensions']
    if isinstance(dimensions, list):
        for dim in dimensions:
            print "Dimension Display Name: {0} - Dimension ID: {1}".format(dim['display_name'], dim['id'])


'''
** Retrieve Reporting Filters and Values API Call **
Get the Custom Filters and Values to use in query
Scenario: Developer needs to get the Custom Dimension ID(s) to use within a specific report.
'''
import requests
import urllib

API_KEY = ''
ENDPOINT = '/api/v2.0/reporting/filters?'
BASE_REPORTING_URL = 'https://api.singular.net{0}'.format(ENDPOINT)

params = {
    'api_key': API_KEY
}

result = requests.request('GET', BASE_REPORTING_URL, params=params)
if result and result.json().get('value', {}).get('dimensions', {}):
    dimensions = result.json()['value']['dimensions']
    if isinstance(dimensions, list):
        for dim in dimensions:
            print "Dimension Display Name: {0}".format(dim['display_name'])
            print "Dimension Values: {0}\n".format(dim['values'])


'''
** Retrieve Historical Data Updates API Call **
Get the Historical dates to use in update and query reporting endpoint
Scenario: Developer needs to update data with historical stats that were updated by the network.
'''
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


def build_params(network, data, params):
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
        params_separated_by_day = build_params(network, data[network], params)
        print "Creating data for update to database..."
        results += query(params_separated_by_day)
    return results

data_to_update = get_data_for_update()