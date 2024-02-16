import pandas as pd
import os
from sqlalchemy import create_engine



class DataLoader:
    def __init__(self, folder_path, database_uri):
        self.folder_path = folder_path
        self.database_uri = database_uri
        self.file_paths = []
        self.dataset = pd.DataFrame()
    
    def find_excel_files(self):
        for file_name in os.listdir(self.folder_path):
            if file_name.endswith('xlsx'):
                full_path = os.path.join(self.folder_path, file_name)
                self.file_paths.append(full_path)
    
    def load_data_from_files(self):
        for file_path in self.file_paths:
            temp_df = pd.read_excel(file_path, skiprows=7, skipfooter=1, header=None)
            self.dataset = pd.concat([self.dataset, temp_df], ignore_index=True)
        self.dataset.columns = ['nivel', 'codigo', 'nome_unidade', 'idade', 'homens', 'mulheres']
    
    def save_to_sql(self):
        engine = create_engine(self.database_uri)
        self.dataset.to_sql('dados_composicao', con=engine, if_exists='replace', index=False)
    
    def process(self):
        self.find_excel_files()
        self.load_data_from_files()
        self.save_to_sql()

