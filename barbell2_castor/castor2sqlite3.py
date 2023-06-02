import os
import json
import pandas as pd

EXCEL_FILE_DPCA = os.path.join(os.environ['SURFDRIVE'], 'projects/hpb/castor/20230528_castor2sqlite3/ESPRESSO_v2.0_DPCA_excel_export_20230528115039.xlsx')
COLUMNS_TO_SKIP = [
    'Participant Id',
    'Participant Status',
    'Site Abbreviation',
    'Participant Creation Date',
    'dpca_BMI',
]
FIELD_TYPES_TO_SKIP = ['remark', 'calculation']


class CastorToDict:
    COLUMNS_TO_SKIP = [
        'Participant Id',
        'Participant Status',
        'Site Abbreviation',
        'Participant Creation Date',
        'dpca_BMI',
    ]
    FIELD_TYPES_TO_SKIP = ['remark', 'calculation']


"""
data = {
    'field_name: {
        'field_type': '',
        'field_option_group_name': '',
        'field_option_items': [],
        'field_values': [],
    }
}
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
            option_groups[option_group_name][int(row['Option value'])] = row['Option name']
        # load variable definitions
        df_vars = pd.read_excel(self.excel_file, sheet_name='Study variable list')
        data = {}
        for _, row in df_vars.iterrows():
            field_type = row['Original field type']
            if field_type not in CastorToDict.FIELD_TYPES_TO_SKIP:
                field_name = row['Variable name']
                item = {
                    'field_type': field_type,
                    'field_option_group_name': row['Optiongroup name'],
                    'field_option_items': None,
                    'field_values': [],
                }
                if item['field_type'] == 'radio' or item['field_type'] == 'dropdown':
                    item['field_option_items'] = option_groups[item['field_option_group_name']]
                data[field_name] = item
            else: 
                pass
        # load data
        df_data = pd.read_excel(self.excel_file, sheet_name='Study results')
        for _, row in df_data.iterrows():
            for column in row.keys():
                if column not in CastorToDict.COLUMNS_TO_SKIP and not column.endswith('_calc'):
                    value = row[column]
                    if pd.isna(value):
                        value = ''
                    data[column]['field_values'].append(value)
        # check that field_value arrays are all the same length
        required_length = 0
        for column in data.keys():
            length = len(data[column]['field_values'])
            if required_length == 0:
                required_length = length
            if length != required_length:
                raise RuntimeError(f'Length for column {column} is not correct, should be {required_length} but is {length}')            
        return data
        

class DictToSqlite3:
    pass


if __name__ == '__main__':
    def main():
        ce2d = CastorExcelToDict()
        ce2d.execute()
    main()
