#from packages.database_access_package.database_access_loader import ConfigLoaderSqlite
import sqlite3

# Declarando conexão ao banco
#database_connection = ConfigLoaderSqlite('config/database_access.json')
#database_sympla_and_ibge_uri = database_connection.database_sympla_and_ibge_uri

# Conectando ao SQLite database (criando se não existe)
conn = sqlite3.connect('sympla_ibge_composed_data.sqlite3')
cursor = conn.cursor()

# Criando tabela 'dim_tipo_evento'
cursor.execute('''CREATE TABLE IF NOT EXISTS dim_tipo_evento (
                    id_tipo_evento INTEGER PRIMARY KEY AUTOINCREMENT,
                    ds_tipo_ingresso TEXT NOT NULL
                    )''')

# Criando tabela 'dim_segmento'
cursor.execute('''CREATE TABLE IF NOT EXISTS dim_segmento (
                    id_segmento INTEGER PRIMARY KEY AUTOINCREMENT,
                    ds_segmento_evento TEXT NOT NULL
                    )''')

# Criando tabela 'dim_categoria'
cursor.execute('''CREATE TABLE IF NOT EXISTS dim_categoria (
                    id_categoria INTEGER PRIMARY KEY AUTOINCREMENT,
                    ds_categoria_evento TEXT NOT NULL
                    )''')

# Criando tabela 'dim_grupo_produtor'
cursor.execute('''CREATE TABLE IF NOT EXISTS dim_grupo_produtor (
                    id_grupo INTEGER PRIMARY KEY AUTOINCREMENT,
                    ds_grupo_produtor TEXT NOT NULL
                    )''')

# Criando tabela 'dim_produtor'
cursor.execute('''CREATE TABLE IF NOT EXISTS dim_produtor (
                    id_produtor INTEGER PRIMARY KEY AUTOINCREMENT,
                    id_grupo INTEGER,
                    FOREIGN KEY (id_grupo) REFERENCES dim_grupo_produtor(id_grupo) ON DELETE CASCADE
                    )''')

# Criando tabela 'dim_atividades'
cursor.execute('''CREATE TABLE IF NOT EXISTS dim_atividades (
                    id_atividade INTEGER PRIMARY KEY,
                    descricao_atividade TEXT NOT NULL
                    )''')

# Criando tabela 'dim_cidades'
cursor.execute('''CREATE TABLE IF NOT EXISTS dim_cidades (
                    id_cidade INTEGER PRIMARY KEY,
                    nm_cidade TEXT NOT NULL,
                    sigla_UF TEXT NOT NULL,
                    pib_bruto NUMBER NOT NULL,
                    pib_per_capta NUMBER NOT NULL,
                    atividade_principal_contribuicao INTEGER,
                    atividade_secundaria_contribuicao INTEGER,
                    atividade_tercearia_contribuicao INTEGER,
                    FOREIGN KEY (atividade_principal_contribuicao) REFERENCES dim_atividades(id_atividade) ON DELETE CASCADE,
                    FOREIGN KEY (atividade_secundaria_contribuicao) REFERENCES dim_atividades(id_atividade) ON DELETE CASCADE,
                    FOREIGN KEY (atividade_tercearia_contribuicao) REFERENCES dim_atividades(id_atividade) ON DELETE CASCADE
                    )''')

# Criando tabela 'dim_comp_PIB'
cursor.execute('''CREATE TABLE IF NOT EXISTS dim_comp_PIB (
                    id_cidade INTEGER,
                    idade TEXT NOT NULL,
                    homens INTEGER,
                    mulheres INTEGER,
                    FOREIGN KEY (id_cidade) REFERENCES dim_cidades(id_cidade) ON DELETE CASCADE
                    )''')

# Criando tabela 'ft_eventos'
cursor.execute('''CREATE TABLE IF NOT EXISTS ft_eventos (
                    id_evento VARCHAR PRIMARY KEY,
                    id_produtor INTEGER,
                    id_categoria INTEGER,
                    id_segmento INTEGER,
                    id_tipo_evento INTEGER,
                    id_cidade_evento INTEGER,
                    ds_tipo_ingresso TEXT NOT NULL,
                    fg_ingresso_gratuito INTEGER,
                    vr_ingresso NUMBER NOT NULL,
                    vr_taxa_sympla NUMBER NOT NULL,
                    qt_ingresso_disponibilizado INTEGER NOT NULL,
                    qt_ingresso_vendido INTEGER NOT NULL,
                    dt_evento TEXT NOT NULL
                    )''')

# Commit changes and close connection
conn.commit()
conn.close()

print("Tabelas criadas com sucesso")
