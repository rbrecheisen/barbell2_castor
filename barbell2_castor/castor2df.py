import os
import json
import pandas as pd

from barbell2_castor.api import CastorApiClient


""" -------------------------------------------------------------------------------------------
"""
class CastorToDict:
    
    COLUMNS_TO_SKIP = [
        'Participant Id',
        'Participant Status',
        'Site Abbreviation',
        'Participant Creation Date',
        'dpca_BMI',
    ]
    FIELD_TYPES_TO_SKIP = ['remark', 'calculation']
    
    
""" -------------------------------------------------------------------------------------------
"""
class CastorApiToDict(CastorToDict):
    
    def __init__(self, study_name, client_id, client_secret):
        self.client = CastorApiClient(client_id, client_secret)
        self.study_id = self.client.get_study_id(self.client.get_study(study_name))
    
    def execute(self):
        return self.client.get_study_data(self.study_id)


""" -------------------------------------------------------------------------------------------
"""
class CastorExcelToDict(CastorToDict):

    def __init__(self, excel_file):
        self.excel_file = excel_file
        
    def execute(self):
        
        # load options
        df_opts = pd.read_excel(self.excel_file, sheet_name='Field options')
        option_groups = {}
        for _, row in df_opts.iterrows():
            option_group_name = row['Option group name']
            if option_group_name not in option_groups.keys():
                option_groups[option_group_name] = {}
            option_groups[option_group_name][str(row['Option value'])] = row['Option name']
        
        # load variable definitions
        df_vars = pd.read_excel(self.excel_file, sheet_name='Study variable list')
        data = {}
        for _, row in df_vars.iterrows():
            field_type = row['Original field type']
            if field_type in CastorToDict.FIELD_TYPES_TO_SKIP:
                continue
            field_name = row['Variable name']
            data[field_name] = {
                'field_type': field_type,
                'field_options': None,
                'field_values': [],
            }
            if field_type == 'radio' or field_type == 'dropdown':
                data[field_name]['field_options'] = option_groups[row['Optiongroup name']]
            else:
                pass
        
        # load data
        df_data = pd.read_excel(self.excel_file, sheet_name='Study results')
        for _, row in df_data.iterrows():
            for field_name in row.keys():
                if field_name in CastorToDict.COLUMNS_TO_SKIP or field_name.endswith('_calc'):
                    continue
                field_value = str(row[field_name])
                if pd.isna(field_value) or field_value == 'nan':
                    data[field_name]['field_values'].append('')
                else:
                    try:
                        field_type = data[field_name]['field_type']
                        if field_type == 'radio' or field_type == 'dropdown':
                            data[field_name]['field_values'].append(str(int(float(field_value))))
                        elif field_type == 'numeric':
                            data[field_name]['field_values'].append(str(float(field_value)))
                        elif field_type == 'string' or field_type == 'textarea':
                            data[field_name]['field_values'].append(str(field_value))
                        elif field_type == 'date':
                            data[field_name]['field_values'].append(str(field_value))
                        elif field_type == 'year':
                            data[field_name]['field_values'].append(str(int(float(field_value))))
                        else:
                            raise RuntimeError(f'Unknown field type: {field_type}')
                    except:
                        print()
                
        # check that field_value arrays are all the same length
        required_length = 0
        for column in data.keys():
            length = len(data[column]['field_values'])
            if required_length == 0:
                required_length = length
            if length != required_length:
                raise RuntimeError(f'Length for column {column} is not correct, should be {required_length} but is {length}')            

        return data
    
    
class CastorDictToDataFrame:
    
    def __init__(self, data):
        self.data = data
    
    def execute(self):
        data = {}
        # create table columns by expanding option group with one-hot encoding
        for field_name in self.data.keys():
            field_type = self.data[field_name]['field_type']
            if field_type == 'radio' or field_type == 'dropdown':
                field_options = self.data[field_name]['field_options']
                for option_value in field_options.keys():
                    k = f'{field_name}${option_value}'
                    data[k] = []
            else:
                data[field_name] = []
        # add values to columns
        for field_name in self.data.keys():
            field_type = self.data[field_name]['field_type']
            if field_type == 'radio' or field_type == 'dropdown':
                field_options = self.data[field_name]['field_options']
                for value in self.data[field_name]['field_values']:
                    for option_value in field_options.keys():
                        k = f'{field_name}${option_value}'
                        if option_value == value:
                            data[k].append('1')
                        else:
                            data[k].append('0')
            else:
                for value in self.data[field_name]['field_values']:
                    data[field_name].append(value)
        return pd.DataFrame(data=data)
        

if __name__ == '__main__':
    def main():
        CSV_FILE = os.path.join(os.environ['HOME'], 'Desktop/castor.csv')
        CSV_FILE = 'castor.csv'
        CASTOR_STUDY_NAME = 'ESPRESSO_v2.0_DPCA'
        a2d = CastorApiToDict(
            study_name=CASTOR_STUDY_NAME,
            client_id=open(os.path.join(os.environ['HOME'], 'castorclientid.txt')).readline().strip(),
            client_secret=open(os.path.join(os.environ['HOME'], 'castorclientsecret.txt')).readline().strip(),
        )
        data = a2d.execute()
        d2df = CastorDictToDataFrame(data)
        df = d2df.execute()
        df.to_csv(CSV_FILE, index=False, sep=';', decimal='.')
        # EXCEL_FILE_DPCA = os.path.join(os.environ['SURFDRIVE'], 'projects/hpb/castor/20230528_castor2sqlite3/ESPRESSO_v2.0_DPCA_excel_export_20230528115039.xlsx')
        # e2d = CastorExcelToDict(EXCEL_FILE_DPCA)
        # data = e2d.execute()        
        # with open('data.json', 'w') as f:
        #     json.dump(data, f, indent=4)
    main()
