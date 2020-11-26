import datetime as dt
import urllib.request as urllib2
import json
import yaml


BASE_URL = 


def site_url(start_date, end_date, resourceId):
    """Takes in a stock ticker and returns the relevant Yahoo Finance link.
    Args:
        start_date (datetime): the first date we want availability results for
        end_date (datetime): the last date we want availability data for
        resourceId (str): the number associated with the site of interest
    Returns:
        str: the URL which will return the availability of a specific site
    """
    # format to strings for URL string
    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    url = BASE_URL.format(resourceId, start_date, end_date)
    return url


def get_result(start_date, end_date, resourceId):
    '''queries the website and returns the result as a list of dictionaries
    Inputs:
        start_date (datetime): the first date we want availability results for
        end_date (datetime): the last date we want availability data for
        resourceId (str): the number associated with the site of interest
    Returns:
        list: a list of dictionaries containing availability info
    '''
    url = site_url(start_date, end_date, resourceId)
    content = urllib2.urlopen(url).read().decode('utf-8')
    content = content.replace('null', '-99')
    return json.loads(content)


def main():
    start_date = dt.datetime.strptime('2020-12-01', '%Y-%m-%d').date()
    end_date = dt.datetime.strptime('2021-04-12', '%Y-%m-%d').date()
    results = {}
    with open('sites.yaml') as yaml_file:
        #sites = yaml.load(yaml_file, Loader=yaml.FullLoader)
        sites = yaml.load(yaml_file)
    for id in sites.keys():
        result = get_result(start_date, end_date, id)
        date_range = len(result)
        date_list = [start_date + dt.timedelta(days=x) for x in range(date_range)]
        for date, dict in zip(date_list, result):
            dict['date'] = date
        results[id] = result
    for key in results.keys():
        free_dates = []
        for x in range(len(results[key])):
            if results[key][x]['availability'] == 0:
                free_dates.append(results[key][x]['date'].strftime('%Y-%m-%d'))
        print('The free dates for {} are:'.format(sites[key]))
        print(free_dates)
