from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, lit, monotonically_increasing_id
import os

class DataLoaderSymplaSpark:
    def __init__(self, spark, folder_path_sympla, folder_path_ibge_pib, folder_path_ibge_composicao, database_uri):
        self.spark = spark
        self.folder_path_sympla = folder_path_sympla
        self.folder_path_ibge_pib = folder_path_ibge_pib
        self.folder_path_ibge_composicao = folder_path_ibge_composicao
        self.database_uri = database_uri

    def find_csv_files(self, folder_path):
        return [os.path.join(folder_path, file_name) for file_name in os.listdir(folder_path) if file_name.endswith('.csv')]

    def crate_dimensions_sympla(self):
        file_paths_csv = self.find_csv_files(self.folder_path_sympla)
        df_list = [self.spark.read.option("header", "true").csv(file_path, sep='|') for file_path in file_paths_csv]
        self.dataset_bruto_sympla = df_list[0] if df_list else None
        for df in df_list[1:]:
            self.dataset_bruto_sympla = self.dataset_bruto_sympla.union(df)

        if self.dataset_bruto_sympla:
            self.dim_grupo_produtor = self.dataset_bruto_sympla.select("ds_grupo_produtor").distinct().withColumn("id_grupo", monotonically_increasing_id())
            self.dim_categoria = self.dataset_bruto_sympla.select("ds_categoria_evento").distinct().withColumn("id_categoria", monotonically_increasing_id())
            self.dim_segmento = self.dataset_bruto_sympla.select("ds_segmento_evento").distinct().withColumn("id_segmento", monotonically_increasing_id())
            self.dim_tipo_evento = self.dataset_bruto_sympla.select("ds_tipo_evento").distinct().withColumn("id_tipo_evento", monotonically_increasing_id())

            # More dimensions can be created following the pattern above.

    def create_fato_sympla(self):
        # This method should implement the logic to create the ft_eventos DataFrame.
        # As an example, we'll create a simplified version of the DataFrame transformation.
        # Replace with actual business logic for your application.
        
        if not self.dataset_bruto_sympla:
            return
        
        # Example of transforming and joining. Adjust according to actual requirements.
        self.ft_eventos = self.dataset_bruto_sympla.withColumn("nm_cidade_evento_upper", upper(col("nm_cidade_evento")))
        
        # Example join with dim_categoria. Repeat for other dimensions as needed.
        self.ft_eventos = self.ft_eventos.join(self.dim_categoria, self.ft_eventos.ds_categoria_evento == self.dim_categoria.ds_categoria_evento, "left")

        # Perform other necessary transformations and joins here.

    def save_to_sql(self):
        # Example of saving to SQL database. Adjust connection properties as needed.
        # Note: Requires JDBC driver for your database.
        self.dim_grupo_produtor.write.format("jdbc").option("url", self.database_uri).option("dbtable", "dim_grupo_produtor").mode("overwrite").save()
        # Repeat for other DataFrames as necessary.

    def process(self):
        self.crate_dimensions_sympla()
        self.create_fato_sympla()
        self.save_to_sql()

# Main script execution
if __name__ == "__main__":
    spark = SparkSession.builder.appName("DataLoaderSymplaSpark").getOrCreate()

    folder_path_sympla = "/path/to/sympla/csv/files"
    folder_path_ibge_pib = "/path/to/ibge/pib/files"
    folder_path_ibge_composicao = "/path/to/ibge/composicao/files"
    database_uri = "jdbc:mysql://your_database_url"

    data_loader = DataLoaderSymplaSpark(spark, folder_path_sympla, folder_path_ibge_pib, folder_path_ibge_composicao, database_uri)
    data_loader.process()

    spark.stop()
