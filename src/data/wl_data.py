import numpy as np
import pandas as pd
import os
import re
import json

class WlDataRawLoader:
    def __init__(self, raw_data_directory):
        self.filenames = [raw_data_directory + filename for filename in os.listdir(raw_data_directory)]
    def load_files(self):
        self.data = pd.DataFrame()
        for filename in self.filenames:
            print(filename)
            SS = re.search(r'SS(\w+)\.',filename).group(1)
            current = pd.read_csv(filename, low_memory=True)
            current['SS'] = SS
            self.data = self.data.append(current)
    def generate_report(self,report_destination):
        self.report = self.data.SS.value_counts().to_dict()
        self.report['total_count'] = int(self.data.SS.count())
        with open(report_destination, 'w', encoding='utf-8') as json_file:
            json.dump(self.report, json_file, indent=2, ensure_ascii=False)
            
