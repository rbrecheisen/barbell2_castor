import os
import json
import pandas as pd

EXCEL_FILE_DPCA = os.path.join(os.environ['SURFDRIVE'], 'projects/hpb/castor/20230528_castor2sqlite3/ESPRESSO_v2.0_DPCA_excel_export_20230528115039.xlsx')


class CastorToDict:
    pass


class CastorExcelToDict:

    def __init__(self):
        pass
    
    def execute(self):
        
        # load options
        df_opts = pd.read_excel(EXCEL_FILE_DPCA, sheet_name='Field options')
        option_groups = {}
        for idx, row in df_opts.iterrows():
            option_group_name = row['Option group name']
            if option_group_name not in option_groups.keys():
                option_groups[option_group_name] = {}
            option_groups[option_group_name][int(row['Option value'])] = row['Option name']
        # print(json.dumps(option_groups, indent=4))

        # load variable definitions
        df_vars = pd.read_excel(EXCEL_FILE_DPCA, sheet_name='Study variable list')
        data = []
        for idx, row in df_vars.iterrows():
            item = {
                'field_name': row['Variable name'],
                'field_type': row['Original field type'],
                'field_option_group_name': row['Optiongroup name'],
                'field_option_items': None,
                'field_values': [],
            }
            if item['field_type'] == 'radio' or item['field_type'] == 'dropdown':
                item['field_option_items'] = option_groups[item['field_option_group_name']]
            data.append(item)
        # print(json.dumps(data, indent=4))
            
        # load data
        df_data = pd.read_excel(EXCEL_FILE_DPCA, sheet_name='Study results')
        for idx, row in df_data.iterrows():
            pass
        

class DictToSqlite3:
    pass


if __name__ == '__main__':
    def main():
        ce2d = CastorExcelToDict()
        ce2d.execute()
    main()
