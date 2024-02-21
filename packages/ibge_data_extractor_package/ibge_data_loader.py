import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np

class DataLoaderPIB:
    def __init__(self, folder_path, database_uri):
        self.folder_path = folder_path
        self.database_uri = database_uri
        self.file_paths = []
        self.dataset = pd.DataFrame()
        self.dataset_atividade = pd.DataFrame()
        self.filtered_dataset = pd.DataFrame()
        self.dim_cidades = pd.DataFrame()
        self.pib_table = pd.DataFrame()
        self.dim_atividades = pd.DataFrame()
        
    
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
        # Criação da tabela de dimensao das atividades
        atividade_principal_contribuicao = np.array(self.filtered_dataset['atividade_principal_contribuicao'].unique())
        atividade_secundaria_contribuicao = np.array(self.filtered_dataset['atividade_secundaria_contribuicao'].unique())
        atividade_tercearia_contribuicao = np.array(self.filtered_dataset['atividade_tercearia_contribuicao'].unique())
        atividades = np.union1d(atividade_principal_contribuicao, atividade_secundaria_contribuicao, atividade_tercearia_contribuicao)
        
        self.dim_atividades = pd.DataFrame({'descricao_atividade': atividades})
        self.dim_atividades['id_atividade'] = self.dim_atividades.index
        
        # Criação da tabela dimensão de cidades
        temporary_table_raw = self.filtered_dataset.copy()
        temporary_cities_dimension = temporary_table_raw[['codigo_municipio', 'nome_municipio', 'sigla_uf', 'pib_bruto', 'pip_per_capta', 'atividade_principal_contribuicao', 'atividade_secundaria_contribuicao', 'atividade_tercearia_contribuicao']]
        temporary_cities_dimension.columns = ['id_cidade', 'nm_cidade', 'Sigla_UF', 'pib_bruto', 'pip_per_capta', 'atividade_principal_contribuicao', 'atividade_secundaria_contribuicao', 'atividade_tercearia_contribuicao']
        self.dim_cidades = temporary_cities_dimension
        
        
        
    def save_to_sql(self):
        engine = create_engine(self.database_uri)
        self.dim_atividades.to_sql('dim_atividades', con=engine, if_exists='replace', index=False)
        self.dim_cidades.to_sql('dim_cidades', con=engine, if_exists='replace', index=False)
    
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
        self.dim_comp_PIB = pd.DataFrame()
    
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
        self.dataset.columns = ['nivel', 'codigo', 'nm_cidade', 'idade', 'homens', 'mulheres']
        temporary_dataset = self.dataset[['codigo', 'idade', 'homens', 'mulheres']]
        self.dim_comp_PIB = temporary_dataset
        self.dim_comp_PIB.columns = ['id_cidade','idade', 'homens', 'mulheres']
    
    def save_to_sql(self):
        engine = create_engine(self.database_uri)
        self.dim_comp_PIB.to_sql('dim_comp_PIB', con=engine, if_exists='replace', index=False)
    
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