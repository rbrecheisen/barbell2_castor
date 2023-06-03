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
    
    
class CastorApiToDict(CastorToDict):
    
    def __init__(self):
        pass
    
    def execute(self):
        pass


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
            if field_type == 'radio' or field_type == 'dropdown':
                # this is an option group so we create a set of field names for each option item
                option_items = option_groups[row['Optiongroup name']]
                for option_value in option_items.keys():
                    k = field_name + '$' + option_value
                    data[k] = {'field_type': field_type, 'field_values': []}
            else:
                data[field_name] = {'field_type': field_type, 'field_values': []}
        
        # load data
        df_data = pd.read_excel(self.excel_file, sheet_name='Study results')
        for _, row in df_data.iterrows():
            for column in row.keys():
                if column in CastorToDict.COLUMNS_TO_SKIP or column.endswith('_calc'):
                    continue
                column_value = str(row[column])
                # what if this column refers to an option group? for example, dpca_geslacht? how do I
                # detect this? the dictionary data contains name$value items for option groups. if this
                # column refers to an option group, it should be listed in the option_groups dictionary, right?
                if column in option_groups.keys():
                    # column refers to an option group, so there are multiple keys for this column in the
                    # data dictionary. first get the option items for this option group
                    option_values = option_groups[column].keys()
                    for option_value in option_values:
                        k = column + '$' + option_value
                        if option_value == column_value:
                            data[k]['field_values'].append('1')
                        else:
                            data[k]['field_values'].append('0')                        
                else:
                    if pd.isna(column_value):
                        data[column]['field_values'].append('')
                    else:
                        data[column]['field_values'].append(column_value)
                
        # check that field_value arrays are all the same length
        required_length = 0
        for column in data.keys():
            length = len(data[column]['field_values'])
            if required_length == 0:
                required_length = length
            if length != required_length:
                raise RuntimeError(f'Length for column {column} is not correct, should be {required_length} but is {length}')            

        return data
        

if __name__ == '__main__':
    def main():
        ce2d = CastorExcelToDict(EXCEL_FILE_DPCA)
        data = ce2d.execute()        
        with open('data.json', 'w') as f:
            json.dump(data, f, indent=4)
    main()
