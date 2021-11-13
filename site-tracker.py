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
    '''Queries the website and returns the result as a list of dictionaries.

    Args:
        start_date (datetime): the first date we want availability results for
        end_date (datetime): the last date we want availability data for
        resourceId (str): the number associated with the site of interest

    Returns:
        list: a list of dictionaries containing availability info
    '''
    url = site_url(start_date, end_date, resourceId)
    hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
       'Accept-Encoding': 'none',
       'Accept-Language': 'en-US,en;q=0.8',
       'Connection': 'keep-alive'}
    req = urllib2.Request(url, headers=hdr)
    page = urllib2.urlopen(req)
    content = page.read()
    content = content.decode('utf-8')
    content = content.replace('null', '-99')
    return json.loads(content)


def kickoff():
    """Main orchestrator.
    """
    project_id = os.environ['project_id']
    client = bigquery.Client()
    dataset = os.environ['dataset']
    table_name = os.environ['table_name']
    dataset_ref = client.dataset(dataset)
    table_name_ref = bigquery.TableReference(dataset_ref,
                                                table_name)
    site_data_table = client.get_table(table_name_ref)
    start_date = dt.datetime.strptime('2021-11-15', '%Y-%m-%d').date()
    end_date = dt.datetime.strptime('2022-04-15', '%Y-%m-%d').date()
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
    # only write if there's been a change in bookings or it's a new period
    if (len(booked)>0) | (len(newly_available)>0) | (len(df_merged)==0):
        gh.write_to_gbq(df, client, site_data_table)
    if len(newly_available) > 0:
        email = compose_summary_email(newly_available, df_merged)
        eh.send_email(email)


def summary_email_body(all_available):
    """Composes the email body showing current availability.

    Args:
        all_available (df): df with just the current availability. Includes a column with proper HTML formatting.

    Returns:
        str: HTML formatted string which will be the body of the email
    """
    body = ''
    body_base = '{} is available on these dates:<br /><br />{}<br /><br /><br />'
    sites = all_available.site.unique()
    sites.sort(axis=0)
    for site in sites:
        site_df = all_available.loc[all_available.site == site]
        future = site_df.date >  dt.datetime.today().date()
        site_df = site_df.loc[future]
        table = site_df.to_html(columns=['dow', 'dt_str'], header=False,
                                index=False)
        body = body + body_base.format(site, table)
    body = body.replace('&lt;', '<').replace('&gt;', '>')
    return body


def compose_summary_email(newly_available, df_merged):
    """Composes an email showing current availability.

    Args:
        newly_available (df): df with just the most dates which are newly available
        df_merged (df): df containing all current and previous availability

    Returns:
        dict: dict formatted to MailJet API requirements
    """
    # create a table with all currently available days
    all_available = df_merged.loc[df_merged.availability_y==0].copy(deep=True)
    all_available['dt_str'] = all_available.date.apply(str)
    all_available['dow'] = all_available.date.apply(lambda x: x.strftime("%A"))
    # add HTML code to highlight the newly available days
    mask = ((all_available.availability_x != 0) &
            (df_merged.availability_y == 0))
    hls = '<span style="background-color: #BFFF00">'
    hle = '</span>'
    all_available.loc[mask, 'dt_str'] = hls + all_available.loc[mask, 'dt_str'] + hle
    all_available.loc[mask, 'dow'] = hls + all_available.loc[mask, 'dow'] + hle
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
