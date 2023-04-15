import logging
import pandas as pd

# Recompiled version of sqlite3 with larger nr. of supported columns
from pysqlite3 import dbapi2 as sqlite3


logging.basicConfig()
logger = logging.getLogger(__name__)


""" --------------------------------------------------
Run SQL queries on SQL database and return results as 
Pandas dataframe
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


if __name__ == '__main__':
    def main():
        query_engine = CastorQuery('castor.db')
        df = query_engine.execute(
            'SELECT dpca_idcode, dpca_typok$1, dpca_typok$2 FROM data WHERE dpca_typok$1 = 1;')
        print(df.head())
    main()
