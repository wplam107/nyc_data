import configparser
import requests
import math
from datetime import datetime

# Config for retrieving missing latitude and longitude
config = configparser.ConfigParser()
config.read('./.ini')
MAPS_API_KEY = config.get('GCP', 'API_KEY')

# All retrieved results pass through preprocessing before data transformation
def preprocess(result):
    '''
    Preprocessing function
    '''

    establishment = {
        '_id': result['globalid'].strip('{}'),
        'name': result['doing_business_as_dba'],
        'address': result['business_address'],
        'zipcode': result['zip'],
        'license': result['sla_license_type'],
        'tos': datetime.strptime(result['time_of_submission'], '%Y-%m-%dT%H:%M:%S.%f'),
        'lat': None,
        'lng': None,
        'sidewalk': {'status': False},
        'roadway': {'status': False},
        'openstreets': {'status': False},
        'capacity': 0
    }
    
    if result['approved_for_sidewalk_seating'] == 'yes':
        establishment['sidewalk']['status'] = True
        establishment['sidewalk']['length'] = int(result['sidewalk_dimensions_length'])
        establishment['sidewalk']['width'] = int(result['sidewalk_dimensions_width'])
        
    if result['approved_for_roadway_seating'] == 'yes':
        establishment['roadway']['status'] = True
        establishment['roadway']['length'] = int(result['roadway_dimensions_length'])
        establishment['roadway']['width'] = int(result['roadway_dimensions_width'])
        
    if result['seating_interest_sidewalk'] == 'openstreets':
        establishment['openstreets']['status'] = True
    
    if 'latitude' in result.keys():
        establishment['lat'] = float(result['latitude'])
        establishment['lng'] = float(result['longitude'])

    return establishment


#######################
# Models for Database #
#######################
class Establishment:
    def __init__(self, result):
        temp = preprocess(result)
        for k, v in temp.items():
            setattr(self, k, v)

        if self.lat != None:
            self.lat = float(self.lat)
            self.lng = float(self.lng)
        else:
            self._get_lat_lng()

        if self.sidewalk['status']:
            self.sidewalk = {
                'status': self.sidewalk['status'],
                'length': int(self.sidewalk['length']),
                'width': int(self.sidewalk['width'])
            }
            self._est_capacity(self.sidewalk)
        else:
            pass

        if self.roadway['status']:
            self.roadway = {
                'status': self.roadway['status'],
                'length': int(self.roadway['length']),
                'width': int(self.roadway['width'])
            }
            self._est_capacity(self.roadway)
        else:
            pass
    
    def __repr__(self):
        return ('Establishment('\
                f'_id="{self._id}", '\
                f'name="{self.name}", '\
                f'address="{self.address}", '\
                f'zipcode="{self.zipcode}", '\
                f'license="{self.license}", '\
                f'tos={self.tos}, '\
                f'lat={self.lat}, '\
                f'lng={self.lng}, '\
                f'sidewalk={self.sidewalk}, '\
                f'roadway={self.roadway}, '\
                f'openstreets={self.openstreets}, '\
                f'capacity={self.capacity}'\
                ')'
        )

    def __iter__(self):
        yield 'name', self.name
        yield 'address', self.address
        yield 'zipcode', self.zipcode
        yield 'license', self.license
        yield 'tos', self.tos
        yield 'lat', self.lat
        yield 'lng', self.lng
        yield 'sidewalk', self.sidewalk
        yield 'roadway', self.roadway
        yield 'openstreets', self.openstreets
        yield 'capacity', self.capacity

    def _get_lat_lng(self):
        '''
        Function to retrieve geolocation via Google Cloud Map Platform
        '''
        
        base_url = 'https://maps.googleapis.com/maps/api/geocode/json?address='
        address = '+'.join(self.address.split())
        url = base_url + address + '&key=' + MAPS_API_KEY

        r = requests.get(url)
        
        if r.status_code == 200:
            result = r.json()
            location = result['results'][0]['geometry']['location']
            self.lat = location['lat']
            self.lng = location['lng']
        else:
            pass
    
    def _est_capacity(self, area):
        '''
        Function to create capacity estimates
        '''
        
        l = round(area['length'] / 6)
        if l % 2 != 0:
            l = math.floor(l / 2) + 1
        else:
            l = math.floor(l / 2)
            
        w = round(area['width'] / 6)
        if w % 2 != 0:
            w = math.floor(w / 2) + 1
        else: 
            w = math.floor(w / 2)
            
        self.capacity += l * w * 4
