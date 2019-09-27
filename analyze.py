import json
from src.data.wl_data import WlDataRawLoader

raw_data_loader = WlDataRawLoader('data/raw/')
raw_data_loader.load_files()
raw_data_loader.generate_report('reports/raw_data_report.json')