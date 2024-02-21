import pandas as pd
import os
from sqlalchemy import create_engine, text
import numpy as np
from sqlalchemy.types import Integer, Text, Float, String

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
        # Criação da tabela de dimensao das atividades
        atividade_principal_contribuicao = np.array(self.filtered_dataset['atividade_principal_contribuicao'].unique())
        atividade_secundaria_contribuicao = np.array(self.filtered_dataset['atividade_secundaria_contribuicao'].unique())
        atividade_tercearia_contribuicao = np.array(self.filtered_dataset['atividade_tercearia_contribuicao'].unique())
        
        atividades = np.unique(np.concatenate([atividade_principal_contribuicao, atividade_secundaria_contribuicao, atividade_tercearia_contribuicao]))   
     
        
        self.dim_atividades = pd.DataFrame({'descricao_atividade': atividades})
        self.dim_atividades['id_atividade'] = self.dim_atividades.index
        
        # Criação da tabela dimensão de cidades
        temporary_table_raw = self.filtered_dataset.copy()
        temporary_cities_dimension = temporary_table_raw[['codigo_municipio', 'nome_municipio', 'sigla_uf', 'pib_bruto', 'pip_per_capta', 'atividade_principal_contribuicao', 'atividade_secundaria_contribuicao', 'atividade_tercearia_contribuicao']]
        temporary_cities_dimension.columns = ['id_cidade', 'nm_cidade', 'Sigla_UF', 'pib_bruto', 'pip_per_capta', 'atividade_principal_contribuicao_rotulo', 'atividade_secundaria_contribuicao_rotulo', 'atividade_tercearia_contribuicao_rotulo']
        merged_cities_dimension = temporary_cities_dimension.merge(self.dim_atividades, left_on='atividade_principal_contribuicao_rotulo', right_on='descricao_atividade', how='left')
        merged_cities_dimension.rename(columns={'id_atividade': 'atividade_principal_contribuicao'}, inplace=True)
        merged_cities_dimension = temporary_cities_dimension.merge(self.dim_atividades, left_on='atividade_secundaria_contribuicao_rotulo', right_on='descricao_atividade', how='left')
        merged_cities_dimension.rename(columns={'id_atividade': 'atividade_secundaria_contribuicao'}, inplace=True)
        merged_cities_dimension = temporary_cities_dimension.merge(self.dim_atividades, left_on='atividade_tercearia_contribuicao_rotulo', right_on='descricao_atividade', how='left')
        merged_cities_dimension.rename(columns={'id_atividade': 'atividade_tercearia_contribuicao'}, inplace=True)
        self.dim_cidades = merged_cities_dimension
        
        
        
    def save_to_sql(self):
        if not self.dim_atividades.empty:
            engine = create_engine(self.database_uri)
            dtypes = {'id_atividade': Integer, 'descricao_atividade': Text}
            self.dim_atividades.to_sql('dim_atividades', con=engine, if_exists='replace', index=False, dtype=dtypes)
        else:
            print("DataFrame dim_atividades is empty, cannot save to SQL.")

        if not self.dim_cidades.empty:
            engine = create_engine(self.database_uri)
            dtypes = {
                'id_cidade': Integer,  # No need to specify "PRIMARY KEY" here; that's handled by other mechanisms in SQLAlchemy or the DataFrame index
                'nm_cidade': Text,
                'Sigla_UF': Text,
                'pib_bruto': Float,  # Use Float for real numbers
                'pip_per_capta': Float,  # Use Float for real numbers
                'atividade_principal_contribuicao': Integer,
                'atividade_secundaria_contribuicao': Integer,
                'atividade_tercearia_contribuicao': Integer
            }
            self.dim_cidades.to_sql('dim_cidades', con=engine, if_exists='replace', index=False, dtype=dtypes)

            # Define foreign key relationships
            with engine.connect() as con:
                con.execute(text('PRAGMA foreign_keys=on;'))
                con.execute(text('''
                    CREATE TABLE IF NOT EXISTS dim_cidades (
                        id_cidade INTEGER PRIMARY KEY,
                        nm_cidade TEXT NOT NULL,
                        Sigla_UF TEXT NOT NULL,
                        pib_bruto REAL NOT NULL,
                        pip_per_capta REAL NOT NULL,
                        atividade_principal_contribuicao INTEGER,
                        atividade_secundaria_contribuicao INTEGER,
                        atividade_tercearia_contribuicao INTEGER,
                        FOREIGN KEY (atividade_principal_contribuicao) REFERENCES dim_atividades(id_atividade) ON DELETE CASCADE,
                        FOREIGN KEY (atividade_secundaria_contribuicao) REFERENCES dim_atividades(id_atividade) ON DELETE CASCADE,
                        FOREIGN KEY (atividade_tercearia_contribuicao) REFERENCES dim_atividades(id_atividade) ON DELETE CASCADE
                    );
                    '''))
        else:
            print("DataFrame dim_cidades is empty, cannot save to SQL.")
    
    def process(self):
        self.find_excel_files()
        self.load_data_from_files()
        self.create_tables()
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
        
        dtypes = {
            'id_cidade': Integer,
            'idade': Text,
            'homens': Integer,
            'mulheres': Integer
        }

        
        self.dim_comp_PIB.to_sql('dim_comp_PIB', con=engine, if_exists='replace', index=False, dtype=dtypes)
        
        with engine.connect() as con:
            con.execute(text('PRAGMA foreign_keys=on;'))
            con.execute(text('''
                    CREATE TABLE IF NOT EXISTS dim_comp_PIB (
                        id_cidade INTEGER NOT NULL,
                        idade TEXT NOT NULL,
                        homens INTEGER,
                        mulheres INTEGER,
                        FOREIGN KEY (id_cidade) REFERENCES dim_cidades(id_cidade) ON DELETE CASCADE
                        
                    );
                    '''))     
        
    
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