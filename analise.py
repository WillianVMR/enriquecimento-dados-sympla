import pandas as pd
import os

caminho_pasta = '/Users/willianribeiro/Documents/GitHub/enriquecimento-dados-sympla/Data/Dados-IBGE/COMPOSICAO-POPULACAO'

nomes_arquivos = []

def insere_dados_sql(caminho_pasta):
    for nome_do_arquivo in os.listdir(caminho_pasta):
        if str(nome_do_arquivo).endswith('xlsx'):
            caminho_completo = os.path.join(caminho_pasta, nome_do_arquivo)
            nomes_arquivos.append(caminho_completo)
        
    
    return nomes_arquivos

insere_dados_sql(caminho_pasta)

for nome in nomes_arquivos:
    print(nome)

