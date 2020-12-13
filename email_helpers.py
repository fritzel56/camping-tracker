"""Code used to compose emails for and send emails via the MailJet API.
"""
from mailjet_rest import Client
import os


def email_composition(contact_email, contact_name, subject, body):
    """Composes an email in the MJ format. Returns the result

    Args:
        contact_email (str): Email the email is coming from and going to. 
        contact_name (str): Name the email is coming from and going to.
        subject (str): Subject line for the email
        body (str): Body of the email. Should be properly formatted HTML.

    Returns:
        dict: data structure containing the composed email ready for MJ's API
    """
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
                "Subject": subject,
                "HTMLPart": body,
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
