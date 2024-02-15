import pandas as pd
import os
from sqlalchemy import create_engine

caminho_pasta = '/Users/willianribeiro/Documents/GitHub/enriquecimento-dados-sympla/Data/Dados-IBGE/COMPOSICAO-POPULACAO'

nomes_arquivos = []

# Pega todos os caminhos e retorna uma lista dos caminhos para os Datasets
def insere_dados_sql(caminho_pasta):
    for nome_do_arquivo in os.listdir(caminho_pasta):
        if str(nome_do_arquivo).endswith('xlsx'):
            caminho_completo = os.path.join(caminho_pasta, nome_do_arquivo)
            nomes_arquivos.append(caminho_completo)
        
    return nomes_arquivos

caminhos_arquivos = insere_dados_sql(caminho_pasta)


# Cria o dataframe com todos os dados de arquivos da pasta
def criacao_dataframe(caminhos_arquivos):
    
    dataset_composicao = pd.DataFrame()
    
    for nome in caminhos_arquivos:
        armazenador_dataset = pd.read_excel(nome, skiprows=7, skipfooter=1, header=None)
        
        dataset_composicao = pd.concat([dataset_composicao, armazenador_dataset], ignore_index=True)
    
    dataset_composicao.columns = ['nivel', 'codigo', 'nome_unidade', 'idade', 'homens', 'mulheres']
    
    return dataset_composicao 

dataset_final = criacao_dataframe(caminhos_arquivos)

engine = create_engine('sqlite:///dados_sympla.sqlite3')

dataset_final.to_sql('dados_composicao', con=engine, if_exists='replace', index=False)

print(dataset_final.info())   
        
    

