# Camping tracker
Set this up after discovering that a campsite I was interested in was all booked up. The point was to scan their website and notify me if any new availability cropped up (presumably driven by cancellations).

## File Overview
| File | Description |
|------|-------------|
| email_helpers.py | Contains functions to send emails with the MailJet API.  |
| google_helpers.py | Contains functions use to query and write to BigQuery. |
| requirements.txt | Packages needed to run the code. |
| site-tracker.py | Code scrapes the websites, formats the results, and prints out current availability. |

## Setup

### Missing Elements

Two key things are missing from this repo in order to run the code. I've taken these out to lower the chances that organization whose website I'm scrapping doesn't lock me out. I don't think it should be too difficult for someone determined to add these back in.

1. The environment variable BASE_URL needs to be added. This is the URL of the site to scrape.

2. An extra file needs to be added to the repo. It should be titled sites.yaml. The format should be:

site_code: site_name

site_code is the number which can be plugged into the URL to source the data for that site.
site_name is the name the user wants associated with the site

### Google Cloud Setup

The job uses the same Google Cloud infrastructure used in the [hot-potatoes repo](https://github.com/fritzel56/hot-potatoes). Please review the detailed instructions there for a full walk through.

Here's a high level overview of the infrastructure used:

trigger: Cloud Scheduler

compute: Cloud Functions

storage: BigQuery

email: Mailjet

Key environment variables:
* contact_email: the email to send updates from and to
* contact_name: the name the emails should be addressed to
* api_key: your API key for Mailjet
* api_secret: your API secret for Mailjet
* base_url: URL to source the data from
* dataset: the dataset in BigQuery where data is being store
* table_name: the name of the table in BigQuery where data is being store
* project_id: Name of the Google Cloud project associated with the BigQuery datasets.
