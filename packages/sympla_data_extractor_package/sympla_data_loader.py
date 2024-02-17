import pandas as pd
import os
from sqlalchemy import create_engine

class DataLoaderSympla:
    def __init__(self, folder_path, database_uri):
        self.folder_path = folder_path
        self.database_uri = database_uri
        self.file_paths = []
        self.dataset = pd.DataFrame()
        
    def find_csv_files(self):
        for file_name in os.listdir(self.folder_path):
            if file_name.endswith('csv'):
                full_path = os.path.join(self.folder_path, file_name)
                self.file_paths.append(full_path)
    
    def load_data_from_files(self):
        for file_path in self.file_paths:
            # OBS: Em função da baixa memória do pandas teremos que especificar os tipos de dados aqui dentro de pd.read_csv( dtype={0: 'str', 10: 'str'})
            temp_df = pd.read_csv(file_path, sep='|')
            self.dataset = pd.concat([self.dataset, temp_df], ignore_index=True)
    
    def save_to_sql(self):
        engine = create_engine(self.database_uri)
        self.dataset.to_sql('dados_sympla', con=engine, if_exists='replace', index=False)
    
    def process(self):
        self.find_csv_files()
        self.load_data_from_files()
        self.save_to_sql()