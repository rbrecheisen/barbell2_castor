import json
import logging
import pandas as pd

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from barbell2_castor.utils import current_time_secs, elapsed_secs, duration

logger = logging.getLogger('__name__')

class CastorApiClient:

    base_url = 'https://data.castoredc.com'
    token_url = base_url + '/oauth/token'
    api_url = base_url + '/api'

    def __init__(self, client_id, client_secret, verbose=False):
        self.verbose = verbose
        if self.verbose:
            logger.info(f'__init__()')
        self.client_id = client_id
        self.client_secret = client_secret
        self.session = self.create_session(self.client_id, self.client_secret)
        self.studies = self.get_studies()

    def create_session(self, client_id, client_secret):
        if self.verbose:
            logger.info(f'create_session('
                'client_id={client_id}, '
                'client_secret={client_secret}, '
                'token_url={self.token_url})'
            )
        client = BackendApplicationClient(client_id=client_id)
        client_session = OAuth2Session(client=client)
        client_session.fetch_token(
            token_url=self.token_url,
            client_id=client_id,
            client_secret=client_secret,
        )
        return client_session

    def recreate_session(self):
        self.session = self.create_session(self.client_id, self.client_secret)

    def get_studies(self):
        """ Returns list of study objects
        """
        uri = self.api_url + '/study'
        if self.verbose:
            logger.info(f'get_studies() uri={uri}')
        response = self.session.get(uri)
        response_data = response.json()
        if self.verbose:
            logger.info(f'get_studies() response_data={json.dumps(response_data, indent=4)}')
        studies = []
        for study in response_data['_embedded']['study']:
            studies.append(study)
        return studies

    def get_study(self, name):
        """ Returns study object for given name
        :param name Study name
        """
        if self.verbose:
            logger.info(f'get_study(name={name}')
        for study in self.studies:
            if study['name'] == name:
                return study
        return None

    def get_study_id(self, study):
        """ Returns study ID for given study object
        :param study Study object
        """
        if self.verbose:
            if 'study_id' not in study.keys():
                logger.info(f'get_study_id(study={study}) missing key study_id')
        study_id = study['study_id']
        return study_id

    def get_records(self, study_id):
        """ Returns list of records
        :param study_id Study ID
        :param verbose
        """
        record_url = self.api_url + '/study/{}/record'.format(study_id)
        response = self.session.get(record_url)
        response_data = response.json()
        page_count = response_data['page_count']
        records = []
        for i in range(1, page_count + 1):
            record_page_url = self.api_url + '/study/{}/record?page={}'.format(study_id, i)
            response = self.session.get(record_page_url)
            response_data = response.json()
            for record in response_data['_embedded']['records']:
                if not record['id'].startswith('ARCHIVED'):
                    records.append(record)
                    if self.verbose:
                        print(record)
        return records
    
    def get_record_field_data(self, study_id, record_id):
        """ Returns list of all field values for this record 
        :param study_id: Study ID
        :param record_id: Record (or participant) ID
        """
        record_url = self.api_url + '/study/{}/participant/{}/data-points/study'.format(study_id, record_id)
        response = self.session.get(record_url)
        record_field_data = response.json()
        return record_field_data['_embedded']['items']

    @staticmethod
    def get_record_id(record):
        """ Returns record ID for given record object
        :param record Record object
        """
        record_id = record['id']
        return record_id

    def get_fields(self, study_id):
        """ Returns list of field objects defined for the given study
        :param study_id Study ID
        :param verbose
        """
        field_url = self.api_url + '/study/{}/field'.format(study_id)
        response = self.session.get(field_url)
        response_data = response.json()
        page_count = response_data['page_count']
        fields = []
        for i in range(1, page_count + 1):
            field_page_url = self.api_url + '/study/{}/field?page={}'.format(study_id, i)
            response = self.session.get(field_page_url)
            response_data = response.json()
            for field in response_data['_embedded']['fields']:
                fields.append(field)
                if self.verbose:
                    logger.info(field)
        return fields

    @staticmethod
    def get_field(fields, name):
        """ Returns field object of given name
        :param fields List of field objects to search through
        :param name Field name
        """
        for field in fields:
            if field['field_variable_name'] == name:
                return field
        return None

    @staticmethod
    def get_field_id(field):
        """ Returns field ID for given field object
        :param field Field object
        """
        field_id = field['id']
        return field_id

    @staticmethod
    def get_field_type(field):
        """ Returns field type for given field object
        :param field Field object
        """
        field_type = field['field_type']
        return field_type

    def get_field_value(self, study_id, record_id, field_id):
        """ Returns value for given field, record and study
        :param study_id Study ID
        :param record_id Record ID
        :param field_id Field ID for which data is retrieved
        """
        field_data_url = self.api_url + '/study/{}/record/{}/study-data-point/{}'.format(study_id, record_id, field_id)
        response = self.session.get(field_data_url)
        if response.status_code == 200:
            response_data = response.json()
            return response_data['value']
        return None

    def get_study_data(self, study_id):
        """ Returns all study data as a Pandas dataframe
        :param study_id: Study ID
        """
        logger.info('getting study structure...')
        study_structure_url = self.api_url + '/study/{}/export/structure'.format(study_id)
        response = self.session.get(study_structure_url)
        field_defs = {}
        for line in response.text.split('\n')[1:]:
            items = line.split(';')
            if len(items) > 11:
                field_type = items[11]
                if field_type != 'calculation' and field_type != 'remark':
                    field_id = items[8]
                    field_variable_name = items[9]
                    field_defs[field_id] = [field_variable_name, field_type]
        # with open('/Users/Ralph/Desktop/study_structure.json', 'w') as f:
        #     json.dump(field_defs, f, indent=4)
        logger.info('getting study data...')
        study_data_url = self.api_url + '/study/{}/export/data'.format(study_id)
        response = self.session.get(study_data_url)
        records = {}
        for line in response.text.split('\n')[1:]:
            items = line.split(';')
            if len(items) == 9:
                record_id = items[1]
                form_type = items[2]
                if form_type == '':
                    records[record_id] = {}
                elif form_type == 'Study':
                    field_id = items[5]
                    field_value = items[6]
                    records[record_id][field_id] = field_value
        # with open('/Users/Ralph/Desktop/study_data.json', 'w') as f:
        #     json.dump(records, f, indent=4)
        logger.info('building dataset...')
        records_data = {}
        for field_id in field_defs.keys():
            field_variable_name = field_defs[field_id][0]
            field_type = field_defs[field_id][1]
            for record_id in list(records.keys()):  # Make sure we iterate over record IDs in increasing order
                if field_id not in records_data.keys():
                    records_data[field_id] = {'field_variable_name': field_variable_name, 'field_type': field_type, 'field_values': []}
                if field_id in records[record_id].keys():
                    records_data[field_id]['field_values'].append(records[record_id][field_id])
                else:
                    records_data[field_id]['field_values'].append('')
        # print(json.dumps(records_data, indent=4))
        return records_data
