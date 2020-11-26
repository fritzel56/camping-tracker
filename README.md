# Camping tracker
Set this up after discovering that a campsite I was interested in was all booked up. The point was to scan their website and notify me if any new availability cropped up (presumably driven by cancellations).

## File Overview
| File | Description |
|------|-------------|
| site-tracker.py | Code scrapes the websites, formats the results, and prints out current availability. |

## Setup

Two key things are missing from this repo in order to run the code. I've taken these out so the organization whose website I'm scrapping doesn't lock me out but shouldn't be too hard for someone determined to add these back in.

BASE_URL needs to be filled in.
An extra file needs to be added to the repo. It should be titled sites.yaml. The format should be:

site_code: site_name
