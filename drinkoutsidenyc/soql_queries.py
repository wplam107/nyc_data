from sodapy import Socrata
from configparser import ConfigParser

from datetime import datetime

config = ConfigParser()
config.read('./.ini')

TOKEN = config.get('Socrata', 'DRINK_NYC_TOKEN')
DATASET = 'pitm-atqc'
client = Socrata('data.cityofnewyork.us', TOKEN)

def dt_to_string(dt):
    '''
    Function to convert datetime to matching SoQL ISO format
    '''

    assert type(dt) == datetime

    dt = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')

    return dt

def before_query(dt, limit):
    '''
    Query to retrieve all establishments before certain datetime
    '''

    results = client.get(
        DATASET,
        where=f'qualify_alcohol="yes" AND time_of_submission<"{dt}"',
        limit=limit
    )

    return results

def after_query(dt, limit):
    '''
    Query to retrieve all establishments after certain datetime
    '''

    results = client.get(
        DATASET,
        where=f'qualify_alcohol="yes" AND time_of_submission>"{dt}"',
        limit=limit
    )

    return results