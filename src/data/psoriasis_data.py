import pandas as pd
class PsoriasisLabeler:
    def __init__(self, preprocessed_data_location):
        self.preprocessed_data = pd.read_csv(preprocessed_data_location, low_memory=False)
        self.preprocessed_data['F_ENTRADA'] = pd.to_datetime(self.preprocessed_data['F_ENTRADA'], errors='coerce')
        self.preprocessed_data['age'] = self.preprocessed_data['age'].astype(int)
    def label_psoriasis(self, pattern=r'oriasi'):
        self.labeled_data = self.preprocessed_data.copy()
        self.labeled_data['psoriasis'] = False
        self.labeled_data.loc[self.labeled_data['SOSPECHA_DIAG'].str.contains(pattern), 'psoriasis'] = True
    def compute_distribution(self):
        self.psoriasis = self.labeled_data[self.labeled_data.psoriasis == True]
        self.psoriasis_summary = self.psoriasis.groupby([self.psoriasis.F_ENTRADA.dt.year,self.psoriasis.age,self.psoriasis.SEXO,self.psoriasis.SS]).size().to_frame('cases').reset_index()