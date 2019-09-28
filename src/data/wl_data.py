import numpy as np
import pandas as pd
import os
import re
import json
import datetime

class WlDataRawLoader:
    def __init__(self, raw_data_directory):
        self.filenames = [raw_data_directory + filename for filename in os.listdir(raw_data_directory)]
    def load_files(self):
        self.data = pd.DataFrame()
        for filename in self.filenames:
            print(filename)
            SS = re.search(r'SS(\w+)\.',filename).group(1)
            current = pd.read_csv(filename, low_memory=False)
            current = current[['FECHA_NAC', 'F_ENTRADA', 'PRESTA_EST', 'SEXO', 'SOSPECHA_DIAG', 'CONFIR_DIAG']]
            current['SS'] = SS
            current['SOSPECHA_DIAG'] = current['SOSPECHA_DIAG'].map(str) + " " + current['CONFIR_DIAG'].map(str)
            current['FECHA_NAC'] = pd.to_datetime(current['FECHA_NAC'], errors='coerce')
            current['F_ENTRADA'] = pd.to_datetime(current['F_ENTRADA'], errors='coerce')
            current['age'] = (current['F_ENTRADA'] - current['FECHA_NAC'])/datetime.timedelta(days=365)
            current["SEXO"] = current["SEXO"].map(str)
            current.loc[current['SEXO'].str.contains(r'(1|MASCULINO|Hombre)'), 'SEXO'] = 'm'
            current.loc[current['SEXO'].str.contains(r'(2|FEMENINO|Mujer)'), 'SEXO'] = 'f'
            current.loc[current['SEXO'].str.contains(r'^(m|f)$') == False, 'SEXO'] = np.nan
            self.data = self.data.append(current)
        self.data = self.data[['FECHA_NAC', 'F_ENTRADA', 'SEXO', 'SOSPECHA_DIAG']]
    def generate_report(self,report_destination):
        self.report = self.data.SS.value_counts().to_dict()
        self.report['total_count'] = int(self.data.SS.count())
        with open(report_destination, 'w', encoding='utf-8') as json_file:
            json.dump(self.report, json_file, indent=2, ensure_ascii=False)
            
class WlDataRawPreprocessor:
    def __init__(self, raw_data_location):
        self.raw_data = pd.read_csv(raw_data_location)
        self.preprocessing_report = {}
    def preprocess(self):
        self.preprocessing_report['initial_count'] = self.raw_data.count()

        self.data = self.raw_data.dropna()
        self.preprocessing_report['na_dropped'] = self.preprocessing_report['initial_count'] - self.data.count()
        
        self.data = self.data.drop_duplicates()
        self.preprocessing_report['duplicates_dropped'] = self.preprocessing_report['initial_count'] - self.preprocessing_report['na_dropped'] - self.data.count()
    def generate_report(self,preprocessing_report_destination, report_destination):
        self.report = self.data.SS.value_counts().to_dict()
        self.report['total_count'] = int(self.data.SS.count())
        with open(report_destination, 'w', encoding='utf-8') as json_file:
            json.dump(self.report, json_file, indent=2, ensure_ascii=False)
        with open(report_destination, 'w', encoding='utf-8') as json_file:
            json.dump(self.report, json_file, indent=2, ensure_ascii=False)