import datetime as dt
import urllib.request as urllib2
import json
import yaml
import sys
import traceback
from google.cloud import bigquery
import os
import pandas as pd
from mailjet_rest import Client
import google_helpers as gh


def error_composition():
    """Composes an email in the event of an exception with exception details.
    Returns:
        dict: data structure containing the composed email ready for MJ's API
    """
    contact_email = os.environ['contact_email']
    contact_name = os.environ['contact_name']
    err = sys.exc_info()
    err_message = traceback.format_exception(*err)
    data = {
        'Messages': [
            {
                "From": {
                    "Email": contact_email,
                    "Name": contact_name
                },
                "To": [
                    {
                        "Email": contact_email,
                        "Name": contact_name
                    }
                ],
                "Subject": "There was an error with the yurt tracker run",
                "HTMLPart": "There was an error with the yurt tracker run: {} ".format(err_message),
            }
        ]
    }
    return data


def send_email(email):
    """Takes in a composed email and sends it using the mailjet api

    Args:
        email (dict): dict containing all relevant fields needed by the mailjet API
    """
    api_key = os.environ['api_key']
    api_secret = os.environ['api_secret']
    mailjet = Client(auth=(api_key, api_secret), version='v3.1')
    result = mailjet.send.create(data=email)
    print(result)


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
    url = os.environ['base_url'].format(resourceId, start_date, end_date)
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


def compose_summary_email(df):
    contact_email = os.environ['contact_email']
    contact_name = os.environ['contact_name']
    body = ''
    df.sort_values(by=['date'], inplace=True)
    for site in df.site.unique():
        body = body+site+' is available on these dates:<br /><br />{}<br /><br /><br />'.format(df.loc[df.site == site].to_html(columns=['date_str'], header=False, index=False))
    body = body.replace('&lt;', '<').replace('&gt;', '>')
    data = {
        'Messages': [
            {
                "From": {
                    "Email": contact_email,
                    "Name": contact_name
                },
                "To": [
                    {
                        "Email": contact_email,
                        "Name": contact_name
                    }
                ],
                "Subject": "Current Yurt Availability",
                "HTMLPart": body,
            }
        ]
    }
    return data


def kickoff():
    project_id = os.environ['project_id']
    client = bigquery.Client()
    dataset = os.environ['dataset']
    table_name = os.environ['table_name']
    dataset_ref = client.dataset(dataset)
    table_name_ref = bigquery.TableReference(dataset_ref,
                                                table_name)
    site_data_table = client.get_table(table_name_ref)
    start_date = dt.datetime.strptime('2020-12-01', '%Y-%m-%d').date()
    end_date = dt.datetime.strptime('2021-04-12', '%Y-%m-%d').date()
    results = []
    with open('sites.yaml') as yaml_file:
        sites = yaml.load(yaml_file, Loader=yaml.FullLoader)
    for id in sites.keys():
        result = get_result(start_date, end_date, id)
        date_range = len(result)
        date_list = [start_date + dt.timedelta(days=x) for x in range(date_range)]
        for date, dict in zip(date_list, result):
            dict['date'] = date
            dict['site'] = sites[id]
        results.extend(result)
    df = pd.DataFrame(results)
    df = df[['site', 'date', 'availability']]
    df['now'] = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sql = '''
        SELECT yurt_name, availability_date, availability
        FROM `{0}.{1}.{2}`
        WHERE snap_date = (SELECT max(snap_date) FROM `{0}.{1}.{2}`)
    '''.format(project_id, dataset, table_name)
    df_last_data = gh.get_bq_data(sql, client)
    df_merged = df_last_data.merge(df, left_on=['yurt_name', 'availability_date'], right_on=['site', 'date'])
    # newly available
    available = df_merged.loc[(df_merged.availability_x != 0) & (df_merged.availability_y == 0)]
    for site in available.site.unique():
        print('{} is newly available on:'.format(site))
        print(available.loc[available.site == site, 'date'].to_list())
    # newly booked
    booked = df_merged.loc[(df_merged.availability_x != 1) & (df_merged.availability_y == 1)]
    for site in booked.site.unique():
        print('{} is newly booked on:'.format(site))
        print(booked.loc[booked.site == site, 'date'].to_list())
    if (len(booked)>0) | (len(available)>0):
        gh.write_to_gbq(df, client, site_data_table)
    same = df_merged.loc[(df_merged.availability_x == df_merged.availability_y)]
    for site in same.site.unique():
        print('{} is the same on:'.format(site))
        print(same.loc[same.site == site, 'date'].to_list())
    currently_available = df_merged.loc[df_merged.availability_y == 0]
    currently_available['date_str']=currently_available.date.apply(str)
    if len(available) > 0:
        mask = (currently_available.availability_x != 0) & (df_merged.availability_y == 0)
        currently_available.loc[mask, 'date_str'] = '<span style="background-color: #BFFF00">'+currently_available.loc[mask, 'date_str']+'</span>'
        email = compose_summary_email(currently_available)
        send_email(email)


def main(request):
    """Function which orchestrates the rest of the code
    Args:
        request: passed as part of the Google Function orchestration service.
            Not used.
    """
    try:
        # try to run the main body of code
        kickoff()
    except Exception:
        # if it fails. capture the exception and send out a summary email.
        email = error_composition()
        send_email(email)
