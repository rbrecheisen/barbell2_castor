import os
import logging
import datetime

from barbell2_castor.api import CastorApiClient


logging.basicConfig()
logger = logging.getLogger(__name__)


""" --------------------------------------------------
Extract Castor data via API and stores in Python 
dictionary. Dictionary has the following structure:

"""
class CastorToDict:

    def __init__(
            self,
            study_name, 
            client_id, 
            client_secret, 
            output_db_file='castor.db', 
            add_timestamp=False,
            log_level=logging.INFO, 
        ):
        self.study_name = study_name
        self.client_id = client_id
        self.client_secret = client_secret
        self.output_db_file = output_db_file
        if add_timestamp:
            items = os.path.splitext(self.output_db_file)
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            self.output_db_file = f'{items[0]}-{timestamp}{items[1]}'
        self.log_level = log_level
        logging.root.setLevel(self.log_level)

    def execute(self):
        client = CastorApiClient(self.client_id, self.client_secret)
        study = client.get_study(self.study_name)
        study_id = client.get_study_id(study)
        client.get_study_data(study_id)


""" --------------------------------------------------
Takes Castor data from Excel export file and stores it
in Python dictionary.
"""
class CastorExcelToDict:

    def __init__(self):
        pass

    def execute(self):
        pass


""" --------------------------------------------------
Converts Python dictionary with Castor data to SQLite3
format for easy querying.
"""
class DictToSqlite3:

    def __init__(self):
        pass

    def get_sql_object_for_field_value(field_value, i):
        return field_value

    def generate_list_of_sql_statements_for_inserting_records(self):
        data = {
            'dpca_typok$1': {
                'values': [1, 2, 3, 4, 5],
            },
            'dpca_typok$2': {
                'values': [1, 2, 3, 4, 5],
            },
        }
        placeholders = []
        values = []
        nr_records = 1
        for i in range(nr_records):
            placeholder = 'INSERT INTO data ('
            value = []
            for field_name in data.keys():
                placeholder += field_name + ', '
            placeholder = placeholder[:-2] + ') VALUES ('
            for field_name in data.keys():
                value.append(self.get_sql_object_for_field_value(data[field_name][i], i))
                placeholder += '?, '
            placeholder = placeholder[:-2] + ');'
            placeholders.append(placeholder)
            values.append(value)
        return values, placeholders


""" --------------------------------------------------
"""
if __name__ == '__main__':
    def main():
        extractor = CastorToDict(
            study_name='ESPRESSO_v2.0_DPCA',
            client_id=open(os.path.join(os.environ['HOME'], 'castorclientid.txt')).readline().strip(),
            client_secret=open(os.path.join(os.environ['HOME'], 'castorclientsecret.txt')).readline().strip(),
            output_db_file='/Users/ralph/Desktop/castor.db',
            add_timestamp=False,
            log_level=logging.INFO,
        )
        extractor.execute()
    main()
