from packages.database_access_package.database_access_loader import ConfigLoaderSqlite
from packages.raw_data_access_package.raw_data_access_loader import ConfigLoaderRaw
from packages.ibge_data_extractor_package.ibge_data_loader import DataLoaderComposicao
from packages.ibge_data_extractor_package.ibge_data_loader import DataLoaderPIB

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

# Declaração das instancias de ETL sobre os dados do IBGE para injeção no banco SQL
'''
Seja DataLoaderComposição o ETL configurado para absorver os arquivos separados de composição populacional das cidades
Seja DataLoaderPIB o ETL configurado para absorver os arquivos separados de PIB das cidades e PIB per capta dessas
'''
# Carregamento de dados da composição
data_loader_composicao = DataLoaderComposicao(folder_path=dados_composicao_ibge_path, database_uri=database_sympla_and_ibge_uri)
data_loader_composicao.process()

# Carregamento de dados do PIB
data_loader_pib = DataLoaderPIB(folder_path=dados_pib_ibge_path, database_uri=database_sympla_and_ibge_uri)
data_loader_pib.process()