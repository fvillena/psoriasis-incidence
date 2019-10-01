import json
from src.data.wl_data import WlDataRawLoader
from src.data.wl_data import WlDataPreprocessor
from src.data.wl_data import PsoriasisLabeler

raw_data_loader = WlDataRawLoader('data/raw/')
raw_data_loader.load_files()
raw_data_loader.data.to_csv('data/interim/raw_data.csv', index=False)
raw_data_loader.generate_report('reports/raw_data_report.json')

data_preprocessor = WlDataPreprocessor('data/interim/raw_data.csv')
data_preprocessor.preprocess()
data_preprocessor.data.to_csv('data/interim/preprocessed_data.csv', index=False)
data_preprocessor.generate_report('reports/preprocessing_report.json','reports/preprocessed_data_report.json')

psoriasis_labeler = PsoriasisLabeler('data/interim/preprocessed_data.csv')
psoriasis_labeler.label_psoriasis()
psoriasis_labeler.labeled_data.to_csv('data/interim/labeled_data.csv', index=False)
psoriasis_labeler.compute_distribution()
psoriasis_labeler.psoriasis_summary.to_csv('data/processed/psoriasis_summary.csv', index=False)