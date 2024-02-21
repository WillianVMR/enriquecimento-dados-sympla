import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np

from sqlalchemy.types import Integer, Text, Float, String

class DataLoaderSympla:
    def __init__(self, folder_path_sympla, folder_path_ibge_pib, folder_path_ibge_composicao, database_uri):
        self.folder_path_sympla = folder_path_sympla
        self.folder_path_ibge_pib = folder_path_ibge_pib
        self.folder_path_ibge_composicao = folder_path_ibge_composicao
        self.database_uri = database_uri
        self.file_paths_csv = []
        self.file_paths_excel = []
        
        # Dataframes para consumo e alimentação
        self.cities_projection = pd.DataFrame()
        self.dataset_bruto_sympla = pd.DataFrame()
        self.dim_grupo_produtor = pd.DataFrame()
        self.dim_produtor = pd.DataFrame()
        self.dim_categoria = pd.DataFrame()
        self.dim_segmento = pd.DataFrame()
        self.dim_tipo_evento = pd.DataFrame()
        self.ft_eventos = pd.DataFrame()
        
    def find_csv_files(self):
        for file_name in os.listdir(self.folder_path_sympla):
            if file_name.endswith('csv'):
                full_path = os.path.join(self.folder_path_sympla, file_name)
                self.file_paths_csv.append(full_path)
    
    
    def find_excel_files(self):
        for file_name in os.listdir(self.folder_path_ibge_pib):
            if file_name.endswith('xlsx'):
                full_path = os.path.join(self.folder_path_ibge_pib, file_name)
                self.file_paths_excel.append(full_path)
    
    
    def create_projection_citie_dimension(self):
        for file_path in self.file_paths_excel:
            # Tive que alterar a engine de leitura para 'openpyxl' para os arquivos com extensão .xlsx
            temp_df = pd.read_excel(file_path, engine='openpyxl')
            self.cities_projection = pd.concat([self.cities_projection, temp_df], ignore_index=True)
            self.cities_projection = self.cities_projection.iloc[:, [0, 1, 2, 3, 4, 5, 6, 7, 38, 39, 40, 41, 42]]
        self.cities_projection.columns = ['ano', 'codigo_grande_regiao', 'nome_grande_regiao', 'codigo_uf', 'sigla_uf', 'nome_uf', 'codigo_municipio', 'nome_municipio', 'pib_bruto', 'pip_per_capta', 'atividade_principal_contribuicao', 'atividade_secundaria_contribuicao', 'atividade_tercearia_contribuicao']
        self.cities_projection = self.cities_projection[self.cities_projection['ano'] == 2021]
        self.cities_projection = self.cities_projection[['codigo_municipio', 'nome_municipio']]
        
        
    
    def crate_dimensions_sympla(self):
        # Criando um dataset bruto com os dados da Sympla
        for file_path in self.file_paths_csv:
            temp_df = pd.read_csv(file_path, sep='|')
            self.dataset_bruto_sympla = pd.concat([self.dataset_bruto_sympla, temp_df], ignore_index=True)
        
        # Criando as tabelas dimensão da Sympla
        
        # Tabela dim_grupo_produtor
        self.dim_grupo_produtor = pd.DataFrame({'ds_grupo_produtor': np.array(self.dataset_bruto_sympla['ds_grupo_produtor'].unique())})
        self.dim_grupo_produtor['id_grupo'] = self.dim_grupo_produtor.index
        
        # Tabela dim_produtor
        temporary_dim_produtor = self.dataset_bruto_sympla[['id_produtor', 'ds_grupo_produtor']]
        merged_dim_produtor = temporary_dim_produtor.merge(self.dim_grupo_produtor, left_on='ds_grupo_produtor', right_on='ds_grupo_produtor', how='left')
        self.dim_produtor = merged_dim_produtor
        
        # Tabela dim_categoria
        self.dim_categoria = pd.DataFrame({'ds_categoria_evento': np.array(self.dataset_bruto_sympla['ds_categoria_evento'].unique())})
        self.dim_categoria['id_categoria'] = self.dim_categoria.index
        
        # Tabela dim_segmento
        self.dim_segmento = pd.DataFrame({'ds_segmento_evento': np.array(self.dataset_bruto_sympla['ds_segmento_evento'].unique())})
        self.dim_segmento['id_segmento'] = self.dim_segmento.index

        # Tabela dim_tipo_evento
        self.dim_tipo_evento = pd.DataFrame({'ds_tipo_evento': np.array(self.dataset_bruto_sympla['ds_tipo_evento'].unique())})
        self.dim_tipo_evento['id_tipo_evento'] = self.dim_tipo_evento.index
        
    def create_fato_sympla(self):
        
        # Criando a tabela fato sympla: ft_eventos
        
        '''
        Colinha das colunas do CSV
        ['id_produtor', 'id_evento', 'dt_evento', 'ds_categoria_evento',
       'ds_segmento_evento', 'ds_tipo_evento', 'cd_uf_evento',
       'nm_cidade_evento', 'cd_latitude_evento', 'cd_longitude_evento',
       'ds_grupo_produtor', 'ds_tipo_ingresso', 'fg_ingresso_gratuito',
       'vr_ingresso', 'vr_taxa_sympla', 'qt_ingresso_disponibilizado',
       'qt_ingresso_vendido']
        
        Merges que preciso fazer:
        ds_categoria_evento on id_categoria
        ds_segmento_evento on id_segmento
        ds_tipo_evento on id_tipo_evento
        
        
        
        nm_cidade_evento on
        
        '''
        
        temporary_ft_eventos = self.dataset_bruto_sympla[['id_produtor', 'id_evento', 'ds_categoria_evento', 'ds_segmento_evento', 'ds_tipo_evento', 'nm_cidade_evento', 'ds_tipo_ingresso', 'fg_ingresso_gratuito', 'vr_ingresso', 'vr_taxa_sympla', 'qt_ingresso_disponibilizado',
       'qt_ingresso_vendido', 'dt_evento']]
        merged_ft_eventos = temporary_ft_eventos.merge(self.dim_categoria, left_on='ds_categoria_evento', right_on='ds_categoria_evento', how='left')
        merged_ft_eventos = merged_ft_eventos.merge(self.dim_segmento, left_on='ds_segmento_evento', right_on='ds_segmento_evento', how='left')
        merged_ft_eventos = merged_ft_eventos.merge(self.dim_tipo_evento, left_on='ds_tipo_evento', right_on='ds_tipo_evento', how='left')
        merged_ft_eventos['nm_cidade_evento'] = merged_ft_eventos['nm_cidade_evento'].str.upper()
        merged_ft_eventos['nm_cidade_evento'] = merged_ft_eventos['nm_cidade_evento'].str.strip()
        self.cities_projection['nome_municipio'] = self.cities_projection['nome_municipio'].str.upper()
        self.cities_projection['nome_municipio'] = self.cities_projection['nome_municipio'].str.strip()
        merged_ft_eventos = merged_ft_eventos.merge(self.cities_projection, left_on='nm_cidade_evento', right_on='nome_municipio', how='left')
        
        self.ft_eventos = merged_ft_eventos[['id_produtor', 'id_evento', 'id_categoria', 'id_segmento', 'id_tipo_evento', 'codigo_municipio', 'ds_tipo_ingresso', 'fg_ingresso_gratuito', 'vr_ingresso', 'vr_taxa_sympla', 'qt_ingresso_disponibilizado',
       'qt_ingresso_vendido', 'dt_evento']]
        self.ft_eventos.columns = ['id_produtor', 'id_evento', 'id_categoria', 'id_segmento', 'id_tipo_evento', 'id_cidade_evento', 'ds_tipo_ingresso', 'fg_ingresso_gratuito', 'vr_ingresso', 'vr_taxa_sympla', 'qt_ingresso_disponibilizado',
       'qt_ingresso_vendido', 'dt_evento']
        
        
        
    def save_to_sql(self):
        engine = create_engine(self.database_uri)
        self.dim_grupo_produtor.to_sql('dim_grupo_produtor', con=engine, if_exists='replace', index=False)
        self.dim_produtor.to_sql('dim_produtor', con=engine, if_exists='replace', index=False)
        self.dim_categoria.to_sql('dim_categoria', con=engine, if_exists='replace', index=False)
        self.dim_segmento.to_sql('dim_segmento', con=engine, if_exists='replace', index=False)
        self.dim_tipo_evento.to_sql('dim_tipo_evento', con=engine, if_exists='replace', index=False)
        
        dtypes = {
                'id_produtor': Text,
                'id_evento': Integer,
                'id_categoria': Integer,
                'id_segmento': Integer,
                'id_tipo_evento': Integer,
                'id_cidade_evento': Integer,
                'ds_tipo_ingresso': Text,
                'fg_ingresso_gratuito': Integer,
                'vr_ingresso': Float,  # Use Float for real numbers
                'vr_taxa_sympla': Float,  # Use Float for real numbers
                'qt_ingresso_disponibilizado': Integer,
                'qt_ingresso_vendido': Integer,
                'dt_evento': Text
            }
        self.ft_eventos.to_sql('ft_eventos', con=engine, if_exists='replace', index=False, dtype=dtypes)
    
    def process(self):
        self.find_csv_files()
        self.find_excel_files()
        self.create_projection_citie_dimension()
        self.crate_dimensions_sympla()
        self.create_fato_sympla()
        self.save_to_sql()