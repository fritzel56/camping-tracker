import datetime as dt
import urllib.request as urllib2
import json
import yaml
import sys
import traceback
from google.cloud import bigquery
import os
import pandas as pd
import email_helpers as eh
import google_helpers as gh


def site_url(start_date, end_date, resourceId):
    """Takes in a dates and resourceID and returns a URL to query for availability.

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
    newly_available = df_merged.loc[(df_merged.availability_x != 0) & (df_merged.availability_y == 0)]
    booked = df_merged.loc[(df_merged.availability_x != 1) & (df_merged.availability_y == 1)]
    if (len(booked)>0) | (len(newly_available)>0):
        gh.write_to_gbq(df, client, site_data_table)
    if len(newly_available) > 0:
        email = compose_summary_email(newly_available, df_merged)
        eh.send_email(email)


def summary_email_body(all_available):
    body = ''
    body_base = '{} is available on these dates:<br /><br />{}<br /><br /><br />'
    for site in all_available.site.unique():
        site_df = all_available.loc[all_available.site == site]
        table = site_df.to_html(columns=['dt_str'], header=False,
                                index=False)
        body = body + body_base.format(site, table)
    body = body.replace('&lt;', '<').replace('&gt;', '>')
    return body


def compose_summary_email(newly_available, df_merged):
    # create a table with all currently available days
    all_available = df_merged.loc[df_merged.availability_y==0].copy(deep=True)
    all_available['dt_str'] = all_available.date.apply(str)
    # add HTML code to highlight the newly available days
    mask = ((all_available.availability_x != 0) &
            (df_merged.availability_y == 0))
    hls = '<span style="background-color: #BFFF00">'
    hle = '</span>'
    all_available.loc[mask, 'dt_str'] = hls + all_available.loc[mask, 'dt_str'] + hle
    all_available.sort_values(by=['date'], inplace=True)
    # Compose the actual text for the body of the email
    body = summary_email_body(all_available)
    subject = "Current Yurt Availability"
    # Compose the email
    email = eh.email_composition(os.environ['contact_email'],
                                    os.environ['contact_name'],
                                    subject, body)
    return email


def error_email_body():
    """Composes the body of the email in the event of an error with a run.

    Returns:
        str: The errors which caused the job to fail.
    """
    err = sys.exc_info()
    err_message = traceback.format_exception(*err)
    body = "There was an error with a yurt tracker run: {}".format(err_message)
    return body


def main(request):
    """Function which orchestrates the rest of the code
    Args:
        request: passed as part of the Google Function orchestration service.
            Not used.
    """
    try:
        # try to run the main body of code
        kickoff()
        print('Successful run.')
    except Exception:
        # if it fails. capture the exception and send out a summary email.
        print('There was an error with the yurt tracker run.')
        subject = "There was an error with the yurt tracker run"
        body = error_email_body()
        email = eh.email_composition(os.environ['contact_email'],
                                     os.environ['contact_name'],
                                     subject, body)
        eh.send_email(email)
