import json

class ConfigLoaderRaw:
    def __init__(self, config_path):
        self.config_path = config_path
        self.config = self.load_config()
        self.dados_composicao_ibge_path = self.config.get('dados_composicao_ibge_path')
        self.dados_pib_ibge_path = self.config.get('dados_pib_ibge_path')
        self.dados_sympla_eventos_path = self.config.get('dados_sympla_eventos_path')

    def load_config(self):
        try:
            with open(self.config_path, 'r') as config_file:
                config = json.load(config_file)
            return config
        except FileNotFoundError:
            print(f"Configuration file not found: {self.config_path}")
            return {}  # Return an empty dict to avoid breaking the code that depends on the config

# Example usage:
'''
config_loader_raw = ConfigLoaderRaw('config/data_access.json')
dados_composicao_ibge_path = config_loader_raw.dados_composicao_ibge_path
dados_pib_ibge_path = config_loader_raw.dados_pib_ibge_path
dados_sympla_eventos_path = config_loader_raw.dados_sympla_eventos_path

'''