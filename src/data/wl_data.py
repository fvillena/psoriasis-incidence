import numpy as np
import pandas as pd
import dask.dataframe as dd
import os
import re
import json
import datetime

from dask.distributed import Client
client = Client(n_workers=2, threads_per_worker=3, processes=False, memory_limit='5GB')

class WlDataRawLoader:
    def __init__(self, raw_data_directory):
        self.filenames = [raw_data_directory + filename for filename in os.listdir(raw_data_directory)]
    def load_files(self):
        self.data = dd.from_pandas(pd.DataFrame(),8)
        for filename in self.filenames:
            print(filename)
            SS = re.search(r'SS(\w+)\.',filename).group(1)
            current = dd.read_csv(filename, low_memory=False, dtype='object')
            current = current[['FECHA_NAC', 'F_ENTRADA', 'PRESTA_EST', 'SEXO', 'SOSPECHA_DIAG', 'CONFIR_DIAG']]
            current['SS'] = SS
            current['SOSPECHA_DIAG'] = current['SOSPECHA_DIAG'].map(str) + " " + current['CONFIR_DIAG'].map(str)
            current['SOSPECHA_DIAG'] = current['SOSPECHA_DIAG'].str.lower().replace('\n', ' ')
            current['SEXO'] = current['SEXO'].map(str).str.lower()
            current['FECHA_NAC'] = dd.to_datetime(current['FECHA_NAC'], errors='coerce',dayfirst=True, infer_datetime_format=True)
            current['F_ENTRADA'] = dd.to_datetime(current['F_ENTRADA'], errors='coerce',dayfirst=True, infer_datetime_format=True)
            current['age'] = (current['F_ENTRADA'] - current['FECHA_NAC'])/datetime.timedelta(days=365)
            current['SEXO'] = current['SEXO'].mask(current['SEXO'].str.contains(r'(1|masculino|hombre)'), 'm')
            current['SEXO'] = current['SEXO'].mask(current['SEXO'].str.contains(r'(2|femenino|mujer)'), 'f')
            current['SEXO'] = current['SEXO'].mask(current['SEXO'].str.contains(r'^(m|f)$') == False, np.nan)
            self.data = self.data.append(current)
        self.data = self.data[['FECHA_NAC', 'F_ENTRADA', 'age', 'SEXO', 'SOSPECHA_DIAG', 'SS']]
    def generate_report(self,report_destination):
        # self.report = self.data.SS.value_counts().compute().to_dict()
        # self.report['total_count'] = int(self.data.SS.count().compute())
        self.report = {}
        self.report['total_count'] = None
        with open(report_destination, 'w', encoding='utf-8') as json_file:
            json.dump(self.report, json_file, indent=2, ensure_ascii=False)
            
class WlDataPreprocessor:
    def __init__(self, raw_data_location):
        self.raw_data = dd.read_csv(raw_data_location)
        self.preprocessing_report = {}
    def preprocess(self):
        #self.preprocessing_report['initial_count'] = int(self.raw_data.SS.count().compute())

        self.data = self.raw_data.dropna()
        #self.preprocessing_report['na_dropped'] = self.preprocessing_report['initial_count'] - int(self.data.SS.count().compute())
        
        self.data = self.data.drop_duplicates()
        #self.preprocessing_report['duplicates_dropped'] = self.preprocessing_report['initial_count'] - self.preprocessing_report['na_dropped'] - int(self.data.SS.count().compute())
    def generate_report(self,preprocessing_report_destination, report_destination):
        #self.report = self.data.SS.value_counts().compute().to_dict()
        self.report = {}
        #self.report['total_count'] = int(self.data.SS.count().compute())
        with open(report_destination, 'w', encoding='utf-8') as json_file:
            json.dump(self.report, json_file, indent=2, ensure_ascii=False)
        with open(preprocessing_report_destination, 'w', encoding='utf-8') as json_file:
            json.dump(self.preprocessing_report, json_file, indent=2, ensure_ascii=False)