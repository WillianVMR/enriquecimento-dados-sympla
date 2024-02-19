import pandas as pd
import os
from sqlalchemy import create_engine

class DataLoaderPIB:
    def __init__(self, folder_path, database_uri):
        self.folder_path = folder_path
        self.database_uri = database_uri
        self.file_paths = []
        self.dataset = pd.DataFrame()
        self.filtered_dataset = pd.DataFrame()
        self.citie_dimension_table = pd.DataFrame()
        self.pib_table = pd.DataFrame()
    
    def find_excel_files(self):
        for file_name in os.listdir(self.folder_path):
            if file_name.endswith('xlsx'):
                full_path = os.path.join(self.folder_path, file_name)
                self.file_paths.append(full_path)
    
    def load_data_from_files(self):
        for file_path in self.file_paths:
            # Tive que alterar a engine de leitura para 'openpyxl' para os arquivos com extensão .xlsx
            temp_df = pd.read_excel(file_path, engine='openpyxl')
            self.dataset = pd.concat([self.dataset, temp_df], ignore_index=True)
            self.filtered_dataset = self.dataset.iloc[:, [0, 1, 2, 3, 4, 5, 6, 7, 38, 39, 40, 41, 42]]
        self.filtered_dataset.columns = ['ano', 'codigo_grande_regiao', 'nome_grande_regiao', 'codigo_uf', 'sigla_uf', 'nome_uf', 'codigo_municipio', 'nome_municipio', 'pib_bruto', 'pip_per_capta', 'atividade_principal_contribuicao', 'atividade_secundaria_contribuicao', 'atividade_tercearia_contribuicao']
        
    def create_tables(self):
        temp_cities_dimension_table = self.filtered_dataset[['ano', 'codigo_grande_regiao', 'nome_grande_regiao', 'codigo_uf', 'sigla_uf', 'nome_uf', 'codigo_municipio', 'nome_municipio']]
        temp_cities_dimension_table = temp_cities_dimension_table[temp_cities_dimension_table['ano'] == 2021]
        self.citie_dimension_table = temp_cities_dimension_table
        temp_pib_table = self.filtered_dataset[['codigo_municipio', 'pib_bruto', 'pip_per_capta', 'atividade_principal_contribuicao', 'atividade_secundaria_contribuicao', 'atividade_tercearia_contribuicao']]
        self.pib_table = temp_pib_table
    
    def save_to_sql(self):
        engine = create_engine(self.database_uri)
        
        # Create dimensao_cidades table
        self.citie_dimension_table.to_sql('dimensao_cidades', con=engine, if_exists='replace', index=False)
        
        # Create dados_pib table with foreign key constraint
        create_pib_table_query = '''
        CREATE TABLE IF NOT EXISTS dados_pib (
            codigo_municipio INTEGER,
            pib_bruto FLOAT,
            pip_per_capta FLOAT,
            atividade_principal_contribuicao TEXT,
            atividade_secundaria_contribuicao TEXT,
            atividade_tercearia_contribuicao TEXT,
            FOREIGN KEY (codigo_municipio) REFERENCES dimensao_cidades(codigo_municipio)
        );
        '''
        with engine.connect() as conn:
            conn.execute(create_pib_table_query)
        
        # Insert data into dados_pib table
        self.pib_table.to_sql('dados_pib', con=engine, if_exists='replace', index=False)
    
    def process(self):
        self.find_excel_files()
        self.load_data_from_files()
        self.save_to_sql()

# Example usage:
'''
folder_path = '/Users/willianribeiro/Documents/GitHub/enriquecimento-dados-sympla/Data/Dados-IBGE/COMPOSICAO-POPULACAO'
database_uri = 'sqlite:///dados_sympla.sqlite3'
data_loader = DataLoaderComposicao(folder_path, database_uri)
data_loader.process()
'''


# Print the DataFrame info to confirm it's loaded correctly
# print(data_loader.dataset.info())


class DataLoaderComposicao:
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
            # Tive que alterar a engine de leitura para 'openpyxl' para os arquivos com extensão .xlsx
            temp_df = pd.read_excel(file_path, skiprows=7, skipfooter=1, header=None, engine='openpyxl')
            self.dataset = pd.concat([self.dataset, temp_df], ignore_index=True)
        self.dataset.columns = ['nivel', 'codigo', 'nome_unidade', 'idade', 'homens', 'mulheres']
    
    def save_to_sql(self):
        engine = create_engine(self.database_uri)
        self.dataset.to_sql('dados_composicao', con=engine, if_exists='replace', index=False)
    
    def process(self):
        self.find_excel_files()
        self.load_data_from_files()
        self.save_to_sql()
        
# Example usage:
'''
folder_path = '/Users/willianribeiro/Documents/GitHub/enriquecimento-dados-sympla/Data/Dados-IBGE/COMPOSICAO-POPULACAO'
database_uri = 'sqlite:///dados_sympla.sqlite3'
data_loader = DataLoaderComposicao(folder_path, database_uri)
data_loader.process()
'''


# Print the DataFrame info to confirm it's loaded correctly
# print(data_loader.dataset.info())