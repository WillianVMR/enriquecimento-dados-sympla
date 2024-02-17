from pyspark.sql import SparkSession
import os
from sqlalchemy import create_engine

class DataLoaderSymplaSpark:
    def __init__(self, folder_path, database_uri):
        self.folder_path = folder_path
        self.database_uri = database_uri
        self.spark = SparkSession.builder \
            .appName("DataLoaderSymplaSpark") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()
        
    def find_csv_files(self):
        file_paths = [os.path.join(self.folder_path, f) for f in os.listdir(self.folder_path) if f.endswith('.csv')]
        return file_paths
    
    def load_data_from_files(self, file_paths):
        # Obs estamos assumindo que todos os arquivos CSV tem a mesma estrutura
        df = self.spark.read.format("csv").option("header", "true").option("sep", "|").load(file_paths)
        return df
    
    def save_to_sql(self, df):
        properties = {"driver": 'your_driver', "user": 'your_username', "password": 'your_password'}
        df.write.jdbc(url=self.database_uri, table='dados_sympla', mode='overwrite', properties=properties)
    
    def process(self):
        file_paths = self.find_csv_files()
        df = self.load_data_from_files(file_paths=file_paths)
        self.save_to_sql(df)