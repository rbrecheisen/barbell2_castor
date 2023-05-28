import os
import json
import pandas as pd

EXCEL_FILE_DPCA = os.path.join(os.environ['SURFDRIVE'], 'projects/hpb/castor/20230528_castor2sqlite3/ESPRESSO_v2.0_DPCA_excel_export_20230528115039.xlsx')


if __name__ == '__main__':
    def main():
        df_data = pd.read_excel(EXCEL_FILE_DPCA, sheet_name='Study results')
        # df_vars = pd.read_excel(EXCEL_FILE_DPCA, sheet_name='Study variable list')
        # df_opts = pd.read_excel(EXCEL_FILE_DPCA, sheet_name='Field options')
        print(df_data.columns)
    main()
