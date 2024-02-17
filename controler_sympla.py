from packages.database_access_package.database_access_loader import ConfigLoaderSqlite
from packages.raw_data_access_package.raw_data_access_loader import ConfigLoaderRaw
from packages.sympla_pandas_data_extractor_package.sympla_data_loader import DataLoaderSympla

# Declaração das classes para armazenamento dos acessos
''' 
Os acessos são todos configurados em arquivos JSON na pasta config de modo que alteração nos endereços de dados brutos e dados elaborados ficam centralizados
'''
# Bancos de dados
config_loader_sqlite = ConfigLoaderSqlite('config/database_access.json')
database_ibge_isolated_uri = config_loader_sqlite.database_ibge_isolated_uri
database_sympla_isolated_uri = config_loader_sqlite.database_sympla_isolated_uri
database_sympla_and_ibge_uri = config_loader_sqlite.database_sympla_and_ibge_uri

# Dados brutos da extração
config_loader_raw = ConfigLoaderRaw('config/data_access.json')
dados_composicao_ibge_path = config_loader_raw.dados_composicao_ibge_path
dados_pib_ibge_path = config_loader_raw.dados_pib_ibge_path
dados_sympla_eventos_path = config_loader_raw.dados_sympla_eventos_path

# Declaração das instancias de ETL sobre os dados da Sympla para injeção no banco SQL
'''
Seja DataLoaderSympla o ETL configurado para absorver os arquivos provenientes da Query do banco de dados da Sympla
'''
# Carregamento de dados da sympla
data_loader_sympla = DataLoaderSympla(folder_path=dados_sympla_eventos_path, database_uri=database_sympla_and_ibge_uri)
data_loader_sympla.process()
print('Dados da Sympla atualizados')