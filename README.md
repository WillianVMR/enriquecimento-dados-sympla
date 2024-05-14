# Enriquecimento de Dados de Empresa de Eventos

Este repositório contém scripts e ferramentas para enriquecer dados fictícios exportados de uma empresa de Eventos, permitindo a geração de insights valiosos.

## Descrição

Os controladores (`controllers`) criam um banco de dados SQL e extraem dados brutos das pastas `Data`. Eles facilitam o processamento e análise dos dados coletados.

## Instalação

Para instalar as bibliotecas necessárias, execute o seguinte comando no terminal:

```bash
pip install -r requirements.txt
```

## Uso

Para iniciar o aplicativo, use o comando:

```bash
python app.py
```

Certifique-se de estar no diretório base do projeto ao executar este comando.

## Estrutura do Projeto

* `Data/`: Contém os dados brutos exportados.
* `Database/`: Scripts para criação do banco de dados.
* `jupyter-notebooks/`: Notebooks Jupyter para análises exploratórias.
* `packages/`: Dependências do projeto.
* `templates/`: Modelos utilizados no processamento dos dados.
* `app.py`: Script principal para execução do aplicativo.
* `controler_ibge.py`: Controlador para integração com dados do IBGE.
* `controler_sympla_pandas.py`: Controlador para processamento de dados com Pandas.
* `controler_sympla_spark.py`: Controlador para processamento de dados com Spark.

## Licença

Este projeto está licenciado sob a licença MIT. Veja o arquivo LICENSE para mais detalhes.

