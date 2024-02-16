import json

class ConfigLoaderSqlite:
    def __init__(self, config_path):
        self.config_path = config_path
        self.config = self.load_config()
        self.database_ibge_isolated_uri = self.config.get('database_ibge_isolated_uri')
        self.database_sympla_isolated_uri = self.config.get('database_sympla_isolated_uri')
        self.database_sympla_and_ibge_uri = self.config.get('database_sympla_and_ibge_uri')

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
config_loader_sqlite = ConfigLoaderSqlite('config/database_access.json')
database_ibge_isolated_uri = config_loader_sqlite.database_ibge_isolated_uri
database_sympla_isolated_uri = config_loader_sqlite.database_sympla_isolated_uri
database_sympla_and_ibge_uri = config_loader_sqlite.database_sympla_and_ibge_uri

'''
