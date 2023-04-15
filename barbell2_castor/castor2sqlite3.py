import os
import logging
import pandas as pd

# Recompiled version of sqlite3 with larger nr. of supported columns
from pysqlite3 import dbapi2 as sqlite3
from datetime import datetime
from barbell2_castor.api import CastorApiClient


logging.basicConfig()
logger = logging.getLogger(__name__)


""" --------------------------------------------------
Extract Castor data via API and stores in Python 
dictionary. Dictionary has the following structure:

    data = {
        'dpca_idcode': [],
        'dpca_typok$1': [],
        'dpca_typok$2': [],
        'dpca_typok$3': [],
        ...
    }

where the different options of each option group gets its own
column in the final table. 
"""
class CastorToDict:

    def __init__(
            self,
            study_name, 
            client_id, 
            client_secret, 
            log_level=logging.INFO, 
            ):
        self.study_name = study_name
        self.client_id = client_id
        self.client_secret = client_secret
        self.log_level = log_level
        logging.root.setLevel(self.log_level)
        self.data = {}

    def execute(self):
        client = CastorApiClient(self.client_id, self.client_secret)
        study = client.get_study(self.study_name)
        study_id = client.get_study_id(study)
        self.data = client.get_study_data(study_id)
        return self.data


""" --------------------------------------------------
Takes Castor data from Excel export file and stores it
in Python dictionary.
"""
class CastorExcelToDict:

    def __init__(self):
        self.data = {}

    def execute(self):
        return self.data


""" --------------------------------------------------
Converts Python dictionary with Castor data to SQLite3
format for easy querying.
"""
class DictToSqlite3:

    CASTOR_TO_SQL_TYPES = {
        'string': 'TEXT',
        'textarea': 'TEXT',
        'radio': 'TINYINT',
        'dropdown': 'TINYINT',
        'numeric': 'FLOAT',
        'date': 'DATE',
        'year': 'TINYINT',
    }

    def __init__(
            self, 
            data,
            output_db_file='castor.db', 
            add_timestamp=False,
            log_level=logging.INFO, 
            ):
        self.data = data
        self.output_db_file = output_db_file
        if add_timestamp:
            items = os.path.splitext(self.output_db_file)
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            self.output_db_file = f'{items[0]}-{timestamp}{items[1]}'
        self.log_level = log_level
        logging.root.setLevel(self.log_level)

    @staticmethod
    def get_sql_object_for_field_data(field_data, i):
        value = field_data['field_values'][i]
        if (field_data['field_type'] == 'radio' or field_data['field_type'] == 'dropdown' or field_data['field_type'] == 'year') and value != '':
            try:
                return int(value)
            except ValueError:
                print('ValueError (int): name={}, value={}'.format(field_data['field_variable_name'], value))
        if field_data['field_type'] == 'numeric' and value != '':
            try:                
                return float(value)
            except ValueError:
                print('ValueError (float): name={}, value={}'.format(field_data['field_variable_name'], value))
        if field_data['field_type'] == 'date' and value != '':
            try:
                return datetime.strptime(value, '%d-%m-%Y').date()
            except ValueError:
                print('ValueError (date): name={}, value={}'.format(field_data['field_variable_name'], value))
        return str(field_data['field_values'][i])

    def generate_list_of_sql_statements_for_inserting_records(self, data):
        nr_records = len(data[list(data.keys())[0]]['field_values'])
        logger.info(f'nr. records: {nr_records}')
        placeholders = []
        values = []
        for i in range(nr_records):
            placeholder = 'INSERT INTO data ('
            value = []
            for field_name in data.keys():
                placeholder += field_name + ', '
            placeholder = placeholder[:-2] + ') VALUES ('
            for field_name in data.keys():
                value.append(self.get_sql_object_for_field_data(data[field_name], i))
                placeholder += '?, '
            placeholder = placeholder[:-2] + ');'
            placeholders.append(placeholder)
            values.append(value)
        return placeholders, values
    
    def generate_sql_field_from_field_type_and_field_name(self, field_type, field_name):
        return '{} {}'.format(field_name, DictToSqlite3.CASTOR_TO_SQL_TYPES[field_type])

    def generate_sql_for_creating_table(self, data):
        logger.info(f'nr. columns: {len(data.keys())}')
        sql = 'CREATE TABLE data (id INTEGER PRIMARY KEY, '
        for field_name in data.keys():
            field_type = data[field_name]['field_type']
            field_type_sql = self.generate_sql_field_from_field_type_and_field_name(field_type, field_name)
            if field_type_sql is not None:
                sql += field_type_sql + ', '
        sql = sql[:-2] + ');'
        return sql

    @staticmethod
    def generate_sql_for_dropping_table():
        return 'DROP TABLE IF EXISTS data;'

    def create_sql_database(self, data):
        conn = None
        try:
            conn = sqlite3.connect(self.output_db_file)
            cursor = conn.cursor()
            cursor.execute(self.generate_sql_for_dropping_table())
            cursor.execute(self.generate_sql_for_creating_table(data))
            placeholders, values = self.generate_list_of_sql_statements_for_inserting_records(data)
            for i in range(len(placeholders)):
                cursor.execute(placeholders[i], values[i])
            conn.commit()
        except sqlite3.Error as e:
            logger.error(e)
        finally:
            if conn:
                conn.close()

    def execute(self):
        self.create_sql_database(self.data)
        return self.output_db_file


""" --------------------------------------------------
"""
class CastorQuery:

    def __init__(self, db_file):
        self.db = self.load_db(db_file)
        self.output = None

    def __del__(self):
        if self.db:
            self.db.close()
            self.db = None

    def load_db(self, db_file):
        try:
            db = sqlite3.connect(db_file)
            return db
        except sqlite3.Error as e:
            logger.error(e)
        return None

    @staticmethod
    def get_column_names(data):
        column_names = []
        for column in data.description:
            column_names.append(column[0])
        return column_names
    
    def to_csv(self, output_file):
        self.output.to_csv(output_file, sep=';', index=False)

    def execute(self, query):
        self.output = None
        cursor = self.db.cursor()
        data = cursor.execute(query)
        df_data = []
        for result in data:
            df_data.append(result)
        self.output = pd.DataFrame(df_data, columns=self.get_column_names(data))        
        return self.output


""" --------------------------------------------------
"""
if __name__ == '__main__':
    def main():
        c2d = CastorToDict(
            study_name='ESPRESSO_v2.0_DPCA',
            client_id=open(os.path.join(os.environ['HOME'], 'castorclientid.txt')).readline().strip(),
            client_secret=open(os.path.join(os.environ['HOME'], 'castorclientsecret.txt')).readline().strip(),
            log_level=logging.INFO,
        )
        data = c2d.execute()
        d2q = DictToSqlite3(
            data=data,
            output_db_file='castor.db',
            add_timestamp=False,
            log_level=logging.INFO,
        )
        db_file = d2q.execute()
        query_engine = CastorQuery(db_file)
        df = query_engine.execute(
            'SELECT dpca_idcode, dpca_typok$1, dpca_typok$2 FROM data WHERE dpca_typok$1 = 1;')
        print(df.head())
    main()
